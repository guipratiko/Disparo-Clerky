/**
 * Scheduler para gerenciar agendamento de disparos
 * Processa disparos diretamente sem usar jobs
 */

import { DispatchService } from '../services/dispatchService';
import { DispatchSchedule } from '../types/dispatch';
import { TemplateService } from '../services/templateService';
import { processContact, calculateDelay } from '../services/dispatchProcessor';
import { pgPool } from '../config/databases';
import { parseJsonbField } from '../utils/dbHelpers';
import {
  hasStartDatePassed,
  isWithinAllowedHours,
  getScheduleTimezone,
} from '../utils/timezoneHelper';

// Set para rastrear disparos em processamento (evitar processamento duplicado)
const processingDispatches = new Set<string>();

/**
 * Verificar se é dia permitido
 */
const isAllowedDay = (schedule: DispatchSchedule): boolean => {
  const today = new Date().getDay();
  return !schedule.suspendedDays.includes(today);
};

// Funções de verificação movidas para timezoneHelper.ts

/**
 * Processar um disparo - enviar mensagens para todos os contatos
 */
const processDispatch = async (dispatchId: string, userId: string): Promise<void> => {
  if (processingDispatches.has(dispatchId)) {
    return;
  }

  processingDispatches.add(dispatchId);

  try {
    const dispatch = await DispatchService.getById(dispatchId, userId);
    if (!dispatch || dispatch.status !== 'running') {
      return;
    }

    // Verificar se há agendamento e se já passou a hora
    if (dispatch.schedule && dispatch.schedule.startDate) {
      const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
      
      // Verificar se a data/hora de início já passou (considerando timezone)
      if (!hasStartDatePassed(dispatch.schedule, userTimezone)) {
        // Ainda não chegou a hora, não processar ainda
        return;
      }
      
      // Verificar se está dentro do horário permitido (considerando timezone)
      if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
        // Fora do horário permitido, não processar
        return;
      }
      
      // Verificar se é dia permitido
      if (!isAllowedDay(dispatch.schedule)) {
        // Dia suspenso, não processar
        return;
      }
    }

    if (dispatch.stats.sent + dispatch.stats.failed >= dispatch.stats.total) {
      await DispatchService.update(dispatchId, dispatch.userId, {
        status: 'completed',
        completedAt: new Date(),
      });
      return;
    }

    if (!dispatch.templateId) {
      return;
    }

    const template = await TemplateService.getById(dispatch.templateId, dispatch.userId);
    if (!template) {
      return;
    }

    const processedCount = dispatch.stats.sent + dispatch.stats.failed;
    const startIndex = processedCount;
    const speed = dispatch.settings.speed;
    
    for (let i = startIndex; i < dispatch.contactsData.length; i++) {
      const contact = dispatch.contactsData[i];
      
      // Verificar se o disparo ainda está em execução
      const currentDispatch = await DispatchService.getById(dispatchId, dispatch.userId);
      if (!currentDispatch || currentDispatch.status !== 'running') {
        break;
      }

      const updatedStats = currentDispatch.stats;
      if (updatedStats.sent + updatedStats.failed >= updatedStats.total) {
        break;
      }

      await processContact(
        dispatchId,
        dispatch.userId,
        dispatch.instanceName,
        dispatch.templateId,
        contact,
        dispatch.defaultName || null,
        dispatch.settings
      );

      // Delay entre mensagens (exceto a última)
      // Para 'randomized', recalcular delay a cada mensagem para gerar novo valor aleatório
      if (i < dispatch.contactsData.length - 1) {
        const delay = calculateDelay(speed);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    // Verificar se todos foram processados
    const finalDispatch = await DispatchService.getById(dispatchId, dispatch.userId);
    if (finalDispatch) {
      const finalStats = finalDispatch.stats;
      if (finalStats.sent + finalStats.failed >= finalStats.total) {
        await DispatchService.update(dispatchId, dispatch.userId, {
          status: 'completed',
          completedAt: new Date(),
        });
      }
    }
  } catch (error) {
    console.error(`❌ Erro ao processar disparo ${dispatchId}:`, error);
  } finally {
    processingDispatches.delete(dispatchId);
  }
};

/**
 * Processar disparos agendados e running
 */
export const processScheduledDispatches = async (): Promise<void> => {
  const scheduledDispatches = await DispatchService.getScheduledDispatches();

  for (const dispatch of scheduledDispatches) {
    try {
      if (!dispatch.schedule) continue;
      
      // Usar timezone salvo no dispatch (ou padrão)
      const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
      
      // Verificar se a data/hora de início já passou (considerando timezone)
      if (!hasStartDatePassed(dispatch.schedule, userTimezone)) continue;
      if (!isAllowedDay(dispatch.schedule)) continue;

      // Verificar se está dentro do horário permitido (considerando timezone)
      if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
        if (dispatch.status === 'running') {
          await DispatchService.update(dispatch.id, dispatch.userId, { status: 'paused' });
        }
        continue;
      }

      if (dispatch.status === 'paused') {
        await DispatchService.update(dispatch.id, dispatch.userId, { status: 'running' });
      }

      if (dispatch.status === 'pending') {
        await DispatchService.update(dispatch.id, dispatch.userId, {
          status: 'running',
          startedAt: new Date(),
        });
      }
    } catch (error) {
      // Ignorar erros individuais
    }
  }

  // Processar disparos 'running' (incluindo os que têm agendamento)
  // Buscar apenas disparos 'running' que não têm agendamento OU já passou a hora agendada
  const runningDispatches = await pgPool.query(
    `SELECT * FROM dispatches WHERE status = 'running'`
  );

  for (const row of runningDispatches.rows) {
    try {
      // Buscar dispatch completo (inclui userTimezone)
      const dispatch = await DispatchService.getById(row.id, row.user_id);
      if (!dispatch) {
        continue;
      }

      // Se tem agendamento, verificar se já passou a hora antes de processar
      if (dispatch.schedule && dispatch.schedule.startDate) {
        const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
        
        // Verificar se a data/hora de início já passou (considerando timezone)
        if (!hasStartDatePassed(dispatch.schedule, userTimezone)) {
          // Ainda não chegou a hora, não processar ainda
          continue;
        }
        
        // Verificar se está dentro do horário permitido (considerando timezone)
        if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
          // Fora do horário permitido, não processar
          continue;
        }
        
        // Verificar se é dia permitido
        if (!isAllowedDay(dispatch.schedule)) {
          // Dia suspenso, não processar
          continue;
        }
      }

      const processedCount = dispatch.stats.sent + dispatch.stats.failed;
      if (processedCount >= dispatch.stats.total) {
        await DispatchService.update(dispatch.id, dispatch.userId, {
          status: 'completed',
          completedAt: new Date(),
        });
        continue;
      }

      // Processar disparo em background
      processDispatch(dispatch.id, dispatch.userId).catch((error) => {
        console.error(`❌ Erro ao processar disparo ${dispatch.id}:`, error);
        processingDispatches.delete(dispatch.id);
      });
    } catch (error) {
      console.error(`❌ Erro ao processar disparo ${row.id}:`, error);
    }
  }
};

/**
 * Iniciar scheduler
 */
export const startScheduler = async (): Promise<void> => {
  await processScheduledDispatches();

  setInterval(async () => {
    await processScheduledDispatches();
  }, 1000); // Verificar a cada 1 segundo

  console.log('✅ Scheduler de disparos iniciado');
};
