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
  console.log(`🔍 processDispatch: [ENTRADA] Iniciando processamento do disparo ${dispatchId} para usuário ${userId}`);
  
  // NOTA: O dispatchId já foi adicionado ao Set no scheduler antes de chamar esta função
  // Não verificamos aqui para evitar race condition - se foi chamado, deve processar
  // A verificação duplicada estava causando o problema de não processar

  try {
    console.log(`📥 processDispatch: Buscando disparo ${dispatchId} no banco de dados...`);
    const dispatch = await DispatchService.getById(dispatchId, userId);
    
    if (!dispatch) {
      console.error(`❌ processDispatch: Disparo ${dispatchId} não encontrado no banco de dados`);
      processingDispatches.delete(dispatchId);
      return;
    }
    
    if (dispatch.status !== 'running') {
      console.log(`⚠️  processDispatch: Disparo ${dispatchId} não está com status 'running' (status atual: ${dispatch.status})`);
      processingDispatches.delete(dispatchId);
      return;
    }

    console.log(`🔄 processDispatch: Disparo ${dispatchId} encontrado - ${dispatch.contactsData.length} contatos, stats: ${JSON.stringify(dispatch.stats)}`);

    // Verificar se há agendamento e se já passou a hora
    if (dispatch.schedule && dispatch.schedule.startDate) {
      const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
      
      // Verificar se a data/hora de início já passou (considerando timezone)
      if (!hasStartDatePassed(dispatch.schedule, userTimezone)) {
        // Ainda não chegou a hora, não processar ainda
        console.log(`⏰ processDispatch: Disparo ${dispatchId} aguardando horário agendado`);
        return;
      }
      
      // Verificar se está dentro do horário permitido (considerando timezone)
      if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
        // Fora do horário permitido, não processar
        console.log(`⏰ processDispatch: Disparo ${dispatchId} fora do horário permitido`);
        return;
      }
      
      // Verificar se é dia permitido
      if (!isAllowedDay(dispatch.schedule)) {
        // Dia suspenso, não processar
        console.log(`📅 processDispatch: Disparo ${dispatchId} em dia suspenso`);
        return;
      }
    } else {
      console.log(`🚀 processDispatch: Disparo ${dispatchId} sem agendamento, processando imediatamente`);
    }

    if (dispatch.stats.sent + dispatch.stats.failed >= dispatch.stats.total) {
      console.log(`✅ processDispatch: Disparo ${dispatchId} já foi concluído (${dispatch.stats.sent} enviadas + ${dispatch.stats.failed} falhas = ${dispatch.stats.total} total)`);
      await DispatchService.update(dispatchId, dispatch.userId, {
        status: 'completed',
        completedAt: new Date(),
      });
      processingDispatches.delete(dispatchId);
      return;
    }

    if (!dispatch.templateId) {
      console.error(`❌ processDispatch: Disparo ${dispatchId} não tem templateId`);
      processingDispatches.delete(dispatchId);
      return;
    }

    console.log(`📄 processDispatch: Buscando template ${dispatch.templateId}...`);
    const template = await TemplateService.getById(dispatch.templateId, dispatch.userId);
    if (!template) {
      console.error(`❌ processDispatch: Template ${dispatch.templateId} não encontrado`);
      processingDispatches.delete(dispatchId);
      return;
    }
    console.log(`✅ processDispatch: Template ${template.name} encontrado`);

    const speed = dispatch.settings.speed;
    console.log(`⚙️  processDispatch: Velocidade configurada: ${speed}`);
    
    // Processar apenas um contato por vez para evitar duplicação
    // Buscar stats atualizadas para saber qual contato processar
    console.log(`📊 processDispatch: Buscando stats atualizadas do disparo ${dispatchId}...`);
    const currentDispatch = await DispatchService.getById(dispatchId, dispatch.userId);
    if (!currentDispatch || currentDispatch.status !== 'running') {
      console.log(`⚠️  processDispatch: Disparo ${dispatchId} não encontrado ou não está 'running' após buscar stats`);
      processingDispatches.delete(dispatchId);
      return;
    }

    const processedCount = currentDispatch.stats.sent + currentDispatch.stats.failed;
    console.log(`📊 processDispatch: Stats atuais - Enviadas: ${currentDispatch.stats.sent}, Falhas: ${currentDispatch.stats.failed}, Total: ${currentDispatch.stats.total}, Processados: ${processedCount}`);
    
    if (processedCount >= currentDispatch.stats.total) {
      console.log(`✅ processDispatch: Disparo ${dispatchId} já foi concluído (todos processados)`);
      await DispatchService.update(dispatchId, dispatch.userId, {
        status: 'completed',
        completedAt: new Date(),
      });
      processingDispatches.delete(dispatchId);
      return;
    }

    // Processar apenas o próximo contato
    if (processedCount < dispatch.contactsData.length) {
      console.log(`📋 processDispatch: Processando contato ${processedCount + 1} de ${dispatch.contactsData.length}`);
      
      // Verificar novamente as stats ANTES de processar para evitar race condition
      const latestDispatch = await DispatchService.getById(dispatchId, dispatch.userId);
      if (!latestDispatch || latestDispatch.status !== 'running') {
        console.log(`⚠️  processDispatch: Status mudou durante verificação, saindo`);
        processingDispatches.delete(dispatchId);
        return;
      }
      
      const latestProcessedCount = latestDispatch.stats.sent + latestDispatch.stats.failed;
      
      // Se o contato já foi processado por outra chamada, não processar novamente
      if (latestProcessedCount > processedCount) {
        console.log(`⏭️  processDispatch: Contato já foi processado por outra chamada (${latestProcessedCount} > ${processedCount})`);
        processingDispatches.delete(dispatchId);
        return; // Já foi processado, sair
      }
      
      // Se o número de processados mudou, usar o valor atualizado
      const actualProcessedCount = latestProcessedCount;
      if (actualProcessedCount >= dispatch.contactsData.length) {
        console.log(`✅ processDispatch: Todos os contatos já foram processados`);
        processingDispatches.delete(dispatchId);
        return; // Todos já foram processados
      }
      
      const contact = dispatch.contactsData[actualProcessedCount];
      console.log(`👤 processDispatch: Processando contato ${actualProcessedCount + 1}/${dispatch.contactsData.length}: ${contact.phone}${contact.name ? ` (${contact.name})` : ''}`);
      
      // Validar instanceName antes de processar
      if (!dispatch.instanceName) {
        console.error(`❌ processDispatch: Disparo ${dispatchId} não tem instanceName!`);
        await DispatchService.update(dispatchId, dispatch.userId, {
          status: 'failed',
        });
        return;
      }
      
      console.log(`📤 processDispatch: Enviando mensagem para ${contact.phone} via instância ${dispatch.instanceName}`);
      
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
      if (actualProcessedCount + 1 < dispatch.contactsData.length) {
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
    if (error instanceof Error) {
      console.error(`   Mensagem: ${error.message}`);
      console.error(`   Stack: ${error.stack}`);
    }
  } finally {
    processingDispatches.delete(dispatchId);
    console.log(`🗑️  processDispatch: Disparo ${dispatchId} removido do Set de processamento`);
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
  // IMPORTANTE: Selecionar explicitamente instance_name para garantir que está disponível
  const runningDispatches = await pgPool.query(
    `SELECT id, user_id, instance_id, instance_name, template_id, name, status, 
            settings, schedule, contacts_data, stats, default_name, user_timezone,
            created_at, updated_at, started_at, completed_at 
     FROM dispatches WHERE status = 'running'`
  );

  if (runningDispatches.rows.length > 0) {
    console.log(`📋 Scheduler: Encontrados ${runningDispatches.rows.length} disparo(s) com status 'running'`);
  }

  for (const row of runningDispatches.rows) {
    try {
      // Buscar dispatch completo (inclui userTimezone)
      const dispatch = await DispatchService.getById(row.id, row.user_id);
      if (!dispatch) {
        console.log(`⚠️  Scheduler: Disparo ${row.id} não encontrado`);
        continue;
      }

      // Se tem agendamento, verificar se já passou a hora antes de processar
      if (dispatch.schedule && dispatch.schedule.startDate) {
        const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
        
        // Verificar se a data/hora de início já passou (considerando timezone)
        if (!hasStartDatePassed(dispatch.schedule, userTimezone)) {
          // Ainda não chegou a hora, não processar ainda
          console.log(`⏰ Scheduler: Disparo ${dispatch.id} aguardando horário agendado`);
          continue;
        }
        
        // Verificar se está dentro do horário permitido (considerando timezone)
        if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
          // Fora do horário permitido, não processar
          console.log(`⏰ Scheduler: Disparo ${dispatch.id} fora do horário permitido`);
          continue;
        }
        
        // Verificar se é dia permitido
        if (!isAllowedDay(dispatch.schedule)) {
          // Dia suspenso, não processar
          console.log(`📅 Scheduler: Disparo ${dispatch.id} em dia suspenso`);
          continue;
        }
      } else {
        // Disparo sem agendamento - processar imediatamente
        console.log(`🚀 Scheduler: Disparo ${dispatch.id} sem agendamento, processando imediatamente`);
      }

      const processedCount = dispatch.stats.sent + dispatch.stats.failed;
      if (processedCount >= dispatch.stats.total) {
        await DispatchService.update(dispatch.id, dispatch.userId, {
          status: 'completed',
          completedAt: new Date(),
        });
        continue;
      }

      // Verificar se já está sendo processado antes de chamar processDispatch
      if (processingDispatches.has(dispatch.id)) {
        console.log(`⏳ Scheduler: Disparo ${dispatch.id} já está sendo processado, pulando...`);
        continue; // Já está sendo processado, pular
      }

      // Adicionar ao Set ANTES de chamar processDispatch para evitar race condition
      processingDispatches.add(dispatch.id);
      console.log(`✅ Scheduler: Disparo ${dispatch.id} adicionado ao Set, chamando processDispatch...`);

      // Processar disparo em background (não await para não bloquear)
      // IMPORTANTE: processDispatch já verifica se está no Set e adiciona novamente se necessário
      // Então não precisamos nos preocupar com isso aqui
      processDispatch(dispatch.id, dispatch.userId)
        .then(() => {
          console.log(`✅ Scheduler: Processamento do disparo ${dispatch.id} concluído com sucesso`);
        })
        .catch((error) => {
          console.error(`❌ Scheduler: Erro ao processar disparo ${dispatch.id}:`, error);
          if (error instanceof Error) {
            console.error(`   Mensagem: ${error.message}`);
            console.error(`   Stack: ${error.stack}`);
          }
          // Garantir que seja removido do Set em caso de erro
          processingDispatches.delete(dispatch.id);
        });
    } catch (error) {
      console.error(`❌ Erro ao processar disparo ${row.id}:`, error);
    }
  }
};

/**
 * Retomar disparos em andamento após reinicialização do serviço
 * Esta função é chamada na inicialização para garantir que disparos que estavam
 * sendo processados quando o serviço foi reiniciado sejam retomados
 */
export const resumeInProgressDispatches = async (): Promise<void> => {
  console.log('🔄 Verificando disparos em andamento para retomar...');
  
  try {
    // Buscar todos os disparos com status 'running' que não foram concluídos
    const runningDispatches = await pgPool.query(
      `SELECT id, user_id, instance_id, instance_name, template_id, name, status, 
              settings, schedule, contacts_data, stats, default_name, user_timezone,
              created_at, updated_at, started_at, completed_at 
       FROM dispatches 
       WHERE status = 'running' 
       AND (stats->>'sent')::int + (stats->>'failed')::int < (stats->>'total')::int`
    );

    if (runningDispatches.rows.length === 0) {
      console.log('✅ Nenhum disparo em andamento encontrado para retomar');
      return;
    }

    console.log(`📋 Encontrados ${runningDispatches.rows.length} disparo(s) em andamento para retomar`);

    for (const row of runningDispatches.rows) {
      try {
        // Buscar dispatch completo
        const dispatch = await DispatchService.getById(row.id, row.user_id);
        if (!dispatch) {
          console.log(`⚠️  Disparo ${row.id} não encontrado ao tentar retomar`);
          continue;
        }

        const processedCount = dispatch.stats.sent + dispatch.stats.failed;
        const remainingCount = dispatch.stats.total - processedCount;

        console.log(`🔄 Retomando disparo ${dispatch.id} (${dispatch.name})`);
        console.log(`   Progresso: ${processedCount}/${dispatch.stats.total} contatos processados`);
        console.log(`   Restantes: ${remainingCount} contatos`);

        // Verificar se o disparo ainda está válido para processar
        if (processedCount >= dispatch.stats.total) {
          // Todos já foram processados, marcar como concluído
          await DispatchService.update(dispatch.id, dispatch.userId, {
            status: 'completed',
            completedAt: new Date(),
          });
          console.log(`✅ Disparo ${dispatch.id} já estava concluído, marcado como 'completed'`);
          continue;
        }

        // Verificar se tem agendamento e se ainda é válido
        if (dispatch.schedule && dispatch.schedule.startDate) {
          const userTimezone = dispatch.userTimezone || 'America/Sao_Paulo';
          
          // Se ainda não passou a hora, manter como 'running' e aguardar
          if (!hasStartDatePassed(dispatch.schedule, userTimezone)) {
            console.log(`⏰ Disparo ${dispatch.id} aguardando horário agendado`);
            continue;
          }
          
          // Se está fora do horário permitido, pausar
          if (!isWithinAllowedHours(dispatch.schedule, userTimezone)) {
            console.log(`⏰ Disparo ${dispatch.id} fora do horário permitido, pausando...`);
            await DispatchService.update(dispatch.id, dispatch.userId, { status: 'paused' });
            continue;
          }
          
          // Verificar se é dia permitido
          if (!isAllowedDay(dispatch.schedule)) {
            console.log(`📅 Disparo ${dispatch.id} em dia suspenso, pausando...`);
            await DispatchService.update(dispatch.id, dispatch.userId, { status: 'paused' });
            continue;
          }
        }

        // Disparo válido para retomar - será processado pelo scheduler normal
        console.log(`✅ Disparo ${dispatch.id} será retomado pelo scheduler`);
        
      } catch (error) {
        console.error(`❌ Erro ao retomar disparo ${row.id}:`, error);
        // Continuar com os próximos disparos mesmo se um falhar
      }
    }

    console.log(`✅ Verificação de disparos em andamento concluída`);
  } catch (error) {
    console.error(`❌ Erro ao verificar disparos em andamento:`, error);
  }
};

/**
 * Iniciar scheduler
 */
export const startScheduler = async (): Promise<void> => {
  console.log('🔄 Iniciando scheduler de disparos...');
  
  // Primeiro, retomar disparos em andamento
  await resumeInProgressDispatches();
  
  // Depois, processar disparos agendados e running normalmente
  await processScheduledDispatches();

  setInterval(async () => {
    await processScheduledDispatches();
  }, 1000); // Verificar a cada 1 segundo

  console.log('✅ Scheduler de disparos iniciado (verificando a cada 1 segundo)');
};
