/**
 * Service para gerenciamento de Templates
 */

import { pgPool } from '../config/databases';
import { parseJsonbField, stringifyJsonb } from '../utils/dbHelpers';
import { Template, CreateTemplateData, UpdateTemplateData, TemplateType, TemplateContent, SequenceTemplateContent } from '../types/dispatch';

export class TemplateService {
  static async create(data: CreateTemplateData): Promise<Template> {
    const query = `
      INSERT INTO templates (user_id, name, type, content)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `;

    const values = [
      data.userId,
      data.name,
      data.type,
      stringifyJsonb(data.content),
    ];

    const result = await pgPool.query(query, values);

    if (result.rows.length === 0) {
      throw new Error('Falha ao criar template');
    }

    return this.mapRowToTemplate(result.rows[0]);
  }

  static async getById(templateId: string, userId: string): Promise<Template | null> {
    const query = `
      SELECT * FROM templates
      WHERE id = $1 AND user_id = $2
    `;

    const result = await pgPool.query(query, [templateId, userId]);

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToTemplate(result.rows[0]);
  }

  static async getByUserId(userId: string, type?: TemplateType): Promise<Template[]> {
    let query = `
      SELECT * FROM templates
      WHERE user_id = $1
    `;
    const params: any[] = [userId];

    if (type) {
      query += ` AND type = $2`;
      params.push(type);
    }

    query += ` ORDER BY created_at DESC`;

    const result = await pgPool.query(query, params);

    return result.rows.map((row: Record<string, any>) => this.mapRowToTemplate(row));
  }

  static async update(
    templateId: string,
    userId: string,
    data: UpdateTemplateData
  ): Promise<Template | null> {
    const updates: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    if (data.name !== undefined) {
      updates.push(`name = $${paramIndex++}`);
      params.push(data.name);
    }

    if (data.content !== undefined) {
      updates.push(`content = $${paramIndex++}`);
      params.push(stringifyJsonb(data.content));
    }

    if (updates.length === 0) {
      return this.getById(templateId, userId);
    }

    updates.push(`updated_at = CURRENT_TIMESTAMP`);
    params.push(templateId, userId);

    const query = `
      UPDATE templates
      SET ${updates.join(', ')}
      WHERE id = $${paramIndex++} AND user_id = $${paramIndex++}
      RETURNING *
    `;

    const result = await pgPool.query(query, params);

    if (result.rows.length === 0) {
      return null;
    }

    return this.mapRowToTemplate(result.rows[0]);
  }

  static async delete(templateId: string, userId: string): Promise<boolean> {
    const query = `
      DELETE FROM templates
      WHERE id = $1 AND user_id = $2
    `;

    const result = await pgPool.query(query, [templateId, userId]);

    return result.rowCount !== null && result.rowCount > 0;
  }

  static validateSequenceContent(content: SequenceTemplateContent): {
    valid: boolean;
    error?: string;
  } {
    if (!content.steps || !Array.isArray(content.steps)) {
      return { valid: false, error: 'Sequência deve ter uma lista de etapas' };
    }

    if (content.steps.length < 2) {
      return { valid: false, error: 'Sequência deve ter no mínimo 2 etapas' };
    }

    const types = new Set(content.steps.map((step) => step.type));
    if (types.size < 2) {
      return {
        valid: false,
        error: 'Sequência deve ter pelo menos 2 etapas de tipos distintos',
      };
    }

    for (let i = 0; i < content.steps.length; i++) {
      const step = content.steps[i];
      if (!step.type || !step.content) {
        return {
          valid: false,
          error: `Etapa ${i + 1} está incompleta`,
        };
      }

      if (step.delay === undefined || step.delay < 0) {
        return {
          valid: false,
          error: `Etapa ${i + 1} deve ter um delay válido (>= 0)`,
        };
      }

      if (!['seconds', 'minutes', 'hours'].includes(step.delayUnit || 'seconds')) {
        return {
          valid: false,
          error: `Etapa ${i + 1} deve ter uma unidade de delay válida`,
        };
      }
    }

    return { valid: true };
  }

  private static mapRowToTemplate(row: Record<string, any>): Template {
    // Parse content com fallback para um tipo válido de TemplateContent
    const parsedContent = parseJsonbField(row.content, { text: '' }) as TemplateContent;
    return {
      id: row.id,
      userId: row.user_id,
      name: row.name,
      type: row.type as TemplateType,
      content: parsedContent,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }
}

