/**
 * Processador de disparos - processa mensagens diretamente sem jobs
 */

import { requestEvolutionAPI } from '../utils/evolutionAPI';
import { replaceVariablesInContent } from '../utils/variableReplacer';
import { ensureNormalizedPhone } from '../utils/numberNormalizer';
import { TemplateService } from './templateService';
import { DispatchService } from './dispatchService';
import { ContactData } from '../types/dispatch';

/**
 * Calcular delay baseado na velocidade
 * Valores ajustados para anti-detecção do WhatsApp
 */
export const calculateDelay = (speed: string): number => {
  switch (speed) {
    case 'fast':
      return 1000; // 1 segundo - Para listas pequenas
    case 'normal':
      return 30000; // 30 segundos - Recomendado
    case 'slow':
      return 60000; // 1 minuto - Mais seguro
    case 'randomized':
      // Randomized: 55-85 segundos (Anti-detection)
      // Gera um valor aleatório entre 55 e 85 segundos
      return Math.floor(Math.random() * 30000) + 55000; // 55-85 segundos
    default:
      return 30000; // Default: normal (30 segundos)
  }
};

/**
 * Converter número para remoteJid
 */
const phoneToRemoteJid = (phone: string): string => {
  if (phone.includes('@')) {
    return phone;
  }
  return `${phone}@s.whatsapp.net`;
};

/**
 * Remover @s.whatsapp.net do número para usar na Evolution API
 */
const cleanPhoneForEvolutionAPI = (remoteJid: string): string => {
  return remoteJid.replace('@s.whatsapp.net', '');
};

/**
 * Deletar mensagem via Evolution API
 */
const deleteMessage = async (
  instanceName: string,
  remoteJid: string,
  messageId: string
): Promise<void> => {
  await requestEvolutionAPI(
    'DELETE',
    `/chat/deleteMessageForEveryone/${encodeURIComponent(instanceName)}`,
    {
      id: messageId,
      remoteJid: remoteJid,
      fromMe: true,
    }
  );
};

/**
 * Interface para retorno das funções de envio
 */
interface SendResult {
  messageId: string;
  remoteJid: string; // remoteJid real retornado pela Evolution API
}

/**
 * Extrair messageId e remoteJid da resposta da Evolution API
 */
const extractSendResult = (response: any, fallbackRemoteJid: string): SendResult => {
  const responseData = response?.data || response;
  const messageId = responseData?.key?.id || responseData?.messageId;
  const realRemoteJid = responseData?.key?.remoteJid || fallbackRemoteJid;
  
  if (!messageId) {
    throw new Error('Não foi possível obter messageId válido da Evolution API');
  }

  return { messageId, remoteJid: realRemoteJid };
};

/**
 * Enviar mensagem de texto
 */
const sendTextMessage = async (
  instanceName: string,
  remoteJid: string,
  text: string
): Promise<SendResult> => {
  const number = cleanPhoneForEvolutionAPI(remoteJid);
  
  const response = await requestEvolutionAPI(
    'POST',
    `/message/sendText/${encodeURIComponent(instanceName)}`,
    {
      number: number,
      text,
    }
  );

  return extractSendResult(response, remoteJid);
};

/**
 * Enviar imagem
 */
const sendImageMessage = async (
  instanceName: string,
  remoteJid: string,
  imageUrl: string,
  caption?: string
): Promise<SendResult> => {
  const number = cleanPhoneForEvolutionAPI(remoteJid);
  
  const response = await requestEvolutionAPI(
    'POST',
    `/message/sendMedia/${encodeURIComponent(instanceName)}`,
    {
      number: number,
      mediatype: 'image',
      media: imageUrl,
      caption: caption || '',
    }
  );

  return extractSendResult(response, remoteJid);
};

/**
 * Enviar vídeo
 */
const sendVideoMessage = async (
  instanceName: string,
  remoteJid: string,
  videoUrl: string,
  caption?: string
): Promise<SendResult> => {
  const number = cleanPhoneForEvolutionAPI(remoteJid);
  
  const response = await requestEvolutionAPI(
    'POST',
    `/message/sendMedia/${encodeURIComponent(instanceName)}`,
    {
      number: number,
      mediatype: 'video',
      media: videoUrl,
      caption: caption || '',
    }
  );

  return extractSendResult(response, remoteJid);
};

/**
 * Enviar áudio
 */
const sendAudioMessage = async (
  instanceName: string,
  remoteJid: string,
  audioUrl: string
): Promise<SendResult> => {
  const number = cleanPhoneForEvolutionAPI(remoteJid);
  
  const response = await requestEvolutionAPI(
    'POST',
    `/message/sendMedia/${encodeURIComponent(instanceName)}`,
    {
      number: number,
      mediatype: 'audio',
      media: audioUrl,
    }
  );

  return extractSendResult(response, remoteJid);
};

/**
 * Enviar arquivo
 */
const sendFileMessage = async (
  instanceName: string,
  remoteJid: string,
  fileUrl: string,
  fileName: string
): Promise<SendResult> => {
  const number = cleanPhoneForEvolutionAPI(remoteJid);
  
  const response = await requestEvolutionAPI(
    'POST',
    `/message/sendMedia/${encodeURIComponent(instanceName)}`,
    {
      number: number,
      mediatype: 'document',
      media: fileUrl,
      fileName,
    }
  );

  return extractSendResult(response, remoteJid);
};

/**
 * Processar uma etapa de sequência
 */
const processSequenceStep = async (
  instanceName: string,
  remoteJid: string,
  step: { type: string; content: any },
  contact: ContactData,
  defaultName?: string
): Promise<SendResult> => {
  const personalizedContent = replaceVariablesInContent(step.content, contact, defaultName || 'Cliente');

  switch (step.type) {
    case 'text':
      return await sendTextMessage(instanceName, remoteJid, personalizedContent.text);
    case 'image':
      return await sendImageMessage(instanceName, remoteJid, personalizedContent.imageUrl);
    case 'image_caption':
      return await sendImageMessage(instanceName, remoteJid, personalizedContent.imageUrl, personalizedContent.caption);
    case 'video':
      return await sendVideoMessage(instanceName, remoteJid, personalizedContent.videoUrl);
    case 'video_caption':
      return await sendVideoMessage(instanceName, remoteJid, personalizedContent.videoUrl, personalizedContent.caption);
    case 'audio':
      return await sendAudioMessage(instanceName, remoteJid, personalizedContent.audioUrl);
    case 'file':
      return await sendFileMessage(instanceName, remoteJid, personalizedContent.fileUrl, personalizedContent.fileName);
    default:
      throw new Error(`Tipo de etapa não suportado: ${step.type}`);
  }
};

/**
 * Calcular delay de delete em milissegundos
 */
const calculateDeleteDelay = (deleteDelay: number, deleteDelayUnit?: string): number => {
  if (!deleteDelay) return 0;
  
  switch (deleteDelayUnit) {
    case 'seconds':
      return deleteDelay * 1000;
    case 'minutes':
      return deleteDelay * 60 * 1000;
    case 'hours':
      return deleteDelay * 60 * 60 * 1000;
    default:
      return deleteDelay * 1000;
  }
};

/**
 * Processar um contato de um disparo
 */
export const processContact = async (
  dispatchId: string,
  userId: string,
  instanceName: string,
  templateId: string,
  contact: ContactData,
  defaultName: string | null,
  settings: { speed: string; autoDelete?: boolean; deleteDelay?: number; deleteDelayUnit?: string }
): Promise<{ success: boolean; error?: string; messageId?: string }> => {
  try {
    const dispatch = await DispatchService.getById(dispatchId, userId);
    if (!dispatch) {
      return { success: false, error: 'Disparo não encontrado' };
    }

    if (dispatch.status !== 'running') {
      return { success: false, error: 'Disparo não está em execução' };
    }

    const normalizedPhone = ensureNormalizedPhone(contact.phone) || contact.phone;
    const formattedPhone = contact.formattedPhone 
      ? ensureNormalizedPhone(contact.formattedPhone) || contact.formattedPhone
      : normalizedPhone;

    const normalizedContact: ContactData = {
      phone: normalizedPhone,
      name: contact.name,
      formattedPhone,
    };

    const template = await TemplateService.getById(templateId, userId);
    if (!template) {
      return { success: false, error: 'Template não encontrado' };
    }

    const personalizedContent = replaceVariablesInContent(
      template.content,
      normalizedContact,
      defaultName || 'Cliente'
    );

    const remoteJid = phoneToRemoteJid(formattedPhone);
    let sendResult: SendResult | undefined;

    switch (template.type) {
      case 'text':
        sendResult = await sendTextMessage(instanceName, remoteJid, personalizedContent.text);
        break;

      case 'image':
        sendResult = await sendImageMessage(instanceName, remoteJid, personalizedContent.imageUrl);
        break;

      case 'image_caption':
        sendResult = await sendImageMessage(instanceName, remoteJid, personalizedContent.imageUrl, personalizedContent.caption);
        break;

      case 'video':
        sendResult = await sendVideoMessage(instanceName, remoteJid, personalizedContent.videoUrl);
        break;

      case 'video_caption':
        sendResult = await sendVideoMessage(instanceName, remoteJid, personalizedContent.videoUrl, personalizedContent.caption);
        break;

      case 'audio':
        sendResult = await sendAudioMessage(instanceName, remoteJid, personalizedContent.audioUrl);
        break;

      case 'file':
        sendResult = await sendFileMessage(instanceName, remoteJid, personalizedContent.fileUrl, personalizedContent.fileName);
        break;

      case 'sequence':
        let lastResult: SendResult | undefined;
        
        for (const step of personalizedContent.steps) {
          let delayMs = step.delay * 1000;
          if (step.delayUnit === 'minutes') {
            delayMs = step.delay * 60 * 1000;
          } else if (step.delayUnit === 'hours') {
            delayMs = step.delay * 60 * 60 * 1000;
          }
          
          if (delayMs > 0) {
            await new Promise(resolve => setTimeout(resolve, delayMs));
          }

          const stepRemoteJid = lastResult?.remoteJid || remoteJid;
          lastResult = await processSequenceStep(
            instanceName,
            stepRemoteJid,
            step,
            normalizedContact,
            defaultName || undefined
          );
        }
        sendResult = lastResult;
        break;

      default:
        return { success: false, error: `Tipo de template não suportado: ${template.type}` };
    }

    const messageId = sendResult?.messageId;
    const realRemoteJid = sendResult?.remoteJid || remoteJid;

    await DispatchService.updateStats(dispatchId, userId, { sent: 1 });

    // AutoDelete: usar realRemoteJid (retornado pela Evolution API)
    if (settings.autoDelete && messageId && settings.deleteDelay) {
      const deleteDelayMs = calculateDeleteDelay(settings.deleteDelay, settings.deleteDelayUnit);
      
      setTimeout(async () => {
        try {
          await deleteMessage(instanceName, realRemoteJid, messageId);
        } catch (error) {
          console.error(`❌ Erro ao deletar mensagem ${messageId}:`, error);
        }
      }, deleteDelayMs);
    }

    return { success: true, messageId };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
    await DispatchService.updateStats(dispatchId, userId, { failed: 1 });
    return { success: false, error: errorMessage };
  }
};
