/**
 * Service para gerenciamento de Disparos
 */

import { pgPool } from '../config/databases';
import { parseJsonbField, stringifyJsonb } from '../utils/dbHelpers';
import {
  Dispatch,
  DispatchStatus,
  DispatchSettings,
  DispatchSchedule,
  DispatchStats,
  ContactData,
  CreateDispatchData,
  UpdateDispatchData,
} from '../types/dispatch';
import { emitDispatchUpdate } from '../socket/socketClient';

export class DispatchService {
  static async create(data: CreateDispatchData): Promise<Dispatch> {
    const stats: DispatchStats = {
      sent: 0,
      failed: 0,
      invalid: 0,
      total: data.contactsData.length,
    };

    const query = `
      INSERT INTO dispatches (
        user_id, instance_id, instance_name, template_id, name, status,
        settings, schedule, contacts_data, stats, default_name, user_timezone
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING *
    `;

    const values = [
      data.userId,
      data.instanceId,
      data.instanceName, // Salvar instanceName para usar no scheduler
      data.templateId || null,
      data.name,
      'pending',
      stringifyJsonb(data.settings),
      stringifyJsonb(data.schedule),
      stringifyJsonb(data.contactsData),
      stringifyJsonb(stats),
      data.defaultName || null,
      data.userTimezone || 'America/Sao_Paulo', // Salvar timezone do usuário
    ];

    const result = await pgPool.query(query, values);

    if (result.rows.length === 0) {
      throw new Error('Falha ao criar disparo');
    }

    return this.mapRowToDispatch(result.rows[0]);
  }

  static async getById(dispatchId: string, userId: string): Promise<Dispatch | null> {
    const query = `
      SELECT * FROM dispatches
      WHERE id = $1 AND user_id = $2
    `;

    const result = await pgPool.query(query, [dispatchId, userId]);

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToDispatch(result.rows[0]);
  }

  static async getByUserId(
    userId: string,
    status?: DispatchStatus
  ): Promise<Dispatch[]> {
    let query = `
      SELECT * FROM dispatches
      WHERE user_id = $1
    `;
    const params: any[] = [userId];

    if (status) {
      query += ` AND status = $2`;
      params.push(status);
    }

    query += ` ORDER BY created_at DESC`;

    const result = await pgPool.query(query, params);

    return result.rows.map((row) => this.mapRowToDispatch(row));
  }

  static async update(
    dispatchId: string,
    userId: string,
    data: UpdateDispatchData
  ): Promise<Dispatch | null> {
    const updates: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    if (data.name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      params.push(data.name);
    }

    if (data.status !== undefined) {
      updates.push(`status = $${paramIndex++}`);
      params.push(data.status);
    }

    if (data.settings !== undefined) {
      updates.push(`settings = $${paramIndex++}`);
      params.push(stringifyJsonb(data.settings));
    }

    if (data.schedule !== undefined) {
      updates.push(`schedule = $${paramIndex++}`);
      params.push(stringifyJsonb(data.schedule));
    }

    if (data.stats !== undefined) {
      updates.push(`stats = $${paramIndex++}`);
      params.push(stringifyJsonb(data.stats));
    }

    if (data.startedAt !== undefined) {
      updates.push(`started_at = $${paramIndex++}`);
      params.push(data.startedAt);
    }

    if (data.completedAt !== undefined) {
      updates.push(`completed_at = $${paramIndex++}`);
      params.push(data.completedAt);
    }

    if (data.defaultName !== undefined) {
      updates.push(`default_name = $${paramIndex++}`);
      params.push(data.defaultName);
    }

    if (updates.length === 0) {
      return this.getById(dispatchId, userId);
    }

    updates.push(`updated_at = CURRENT_TIMESTAMP`);
    params.push(dispatchId, userId);

    const query = `
      UPDATE dispatches
      SET ${updates.join(', ')}
      WHERE id = $${paramIndex++} AND user_id = $${paramIndex++}
      RETURNING *
    `;

    const result = await pgPool.query(query, params);

    if (result.rows.length === 0) {
      return null;
    }

    const updatedDispatch = this.mapRowToDispatch(result.rows[0]);
    
    try {
      emitDispatchUpdate(userId, updatedDispatch);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
      console.error('Erro ao emitir evento de atualização de disparo:', errorMessage);
    }

    return updatedDispatch;
  }

  static async updateStats(
    dispatchId: string,
    userId: string,
    stats: Partial<DispatchStats>
  ): Promise<Dispatch | null> {
    let statsExpression = `COALESCE(stats, '{"sent": 0, "failed": 0, "invalid": 0, "total": 0}'::jsonb)`;
    const params: any[] = [];
    let paramIndex = 1;

    if (stats.sent !== undefined) {
      statsExpression = `jsonb_set(${statsExpression}, '{sent}', to_jsonb(COALESCE((${statsExpression}->>'sent')::int, 0) + $${paramIndex}::int))`;
      params.push(stats.sent);
      paramIndex++;
    }
    
    if (stats.failed !== undefined) {
      statsExpression = `jsonb_set(${statsExpression}, '{failed}', to_jsonb(COALESCE((${statsExpression}->>'failed')::int, 0) + $${paramIndex}::int))`;
      params.push(stats.failed);
      paramIndex++;
    }
    
    if (stats.invalid !== undefined) {
      statsExpression = `jsonb_set(${statsExpression}, '{invalid}', to_jsonb(COALESCE((${statsExpression}->>'invalid')::int, 0) + $${paramIndex}::int))`;
      params.push(stats.invalid);
      paramIndex++;
    }
    
    if (stats.total !== undefined) {
      statsExpression = `jsonb_set(${statsExpression}, '{total}', to_jsonb($${paramIndex}::int))`;
      params.push(stats.total);
      paramIndex++;
    }

    if (params.length === 0) {
      return this.getById(dispatchId, userId);
    }

    params.push(dispatchId, userId);

    const query = `
      UPDATE dispatches
      SET stats = ${statsExpression},
          updated_at = CURRENT_TIMESTAMP
      WHERE id = $${paramIndex++} AND user_id = $${paramIndex++}
      RETURNING *
    `;

    const result = await pgPool.query(query, params);

    if (result.rows.length === 0) {
      return null;
    }

    const updatedDispatch = this.mapRowToDispatch(result.rows[0]);
    
    try {
      emitDispatchUpdate(userId, updatedDispatch);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
      console.error('Erro ao emitir evento de atualização de disparo:', errorMessage);
    }

    return updatedDispatch;
  }

  static async delete(dispatchId: string, userId: string): Promise<boolean> {
    const query = `
      DELETE FROM dispatches
      WHERE id = $1 AND user_id = $2
    `;

    const result = await pgPool.query(query, [dispatchId, userId]);

    return result.rowCount !== null && result.rowCount > 0;
  }

  static async getScheduledDispatches(): Promise<Dispatch[]> {
    const query = `
      SELECT * FROM dispatches
      WHERE status IN ('pending', 'paused', 'running')
        AND schedule IS NOT NULL
      ORDER BY created_at ASC
    `;

    const result = await pgPool.query(query);

    return result.rows.map((row) => this.mapRowToDispatch(row));
  }

  private static mapRowToDispatch(row: any): Dispatch {
    const settings = parseJsonbField<DispatchSettings>(row.settings, {
      speed: 'normal',
      autoDelete: false,
    });
    const schedule = parseJsonbField<DispatchSchedule | null>(row.schedule, null);
    const contactsData = parseJsonbField<ContactData[]>(row.contacts_data, []);
    const stats = parseJsonbField<DispatchStats>(
      row.stats,
      { sent: 0, failed: 0, invalid: 0, total: 0 }
    );

    return {
      id: row.id,
      userId: row.user_id,
      instanceId: row.instance_id,
      instanceName: row.instance_name || row.instance_id, // Usar instance_name se disponível
      templateId: row.template_id || null,
      name: row.name,
      status: row.status || 'pending',
      settings,
      schedule,
      contactsData,
      stats,
      defaultName: row.default_name || null,
      userTimezone: row.user_timezone || 'America/Sao_Paulo',
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      startedAt: row.started_at || null,
      completedAt: row.completed_at || null,
    };
  }
}

