/**
 * Controller para Disparos
 */

import { Response, NextFunction } from 'express';
import { AuthRequest } from '../middleware/auth';
import {
  createValidationError,
  createNotFoundError,
  handleControllerError,
} from '../utils/errorHelpers';
import { DispatchService } from '../services/dispatchService';
import { DispatchSettings, DispatchSchedule } from '../types/dispatch';
import { TemplateService } from '../services/templateService';
import { validateContacts, filterValidContacts } from '../services/contactValidationService';
import { parseCSVFile, parseInputText } from '../utils/csvParser';
import { ensureNormalizedPhone, ensureNormalizedPhoneList } from '../utils/numberNormalizer';
import { getInstanceInfo } from '../utils/instanceHelper';
import { pgPool } from '../config/databases';
import multer from 'multer';
import { ContactData } from '../types/dispatch';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 5 * 1024 * 1024, // 5MB
  },
  fileFilter: (req: any, file: Express.Multer.File, cb: multer.FileFilterCallback) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Apenas arquivos CSV são permitidos'));
    }
  },
});

export const createDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const {
      instanceId,
      templateId,
      name,
      settings,
      schedule,
      contactsSource,
      contactsData,
      columnIds,
      defaultName,
    } = req.body;

    if (!instanceId || !name || !settings || !contactsSource) {
      const missing = [];
      if (!instanceId) missing.push('instanceId');
      if (!name) missing.push('name');
      if (!settings) missing.push('settings');
      if (!contactsSource) missing.push('contactsSource');
      return next(createValidationError(`Campos obrigatórios faltando: ${missing.join(', ')}`));
    }

    if (!templateId) {
      return next(createValidationError('Template é obrigatório para criar disparo'));
    }

    if (!settings.speed || !['fast', 'normal', 'slow', 'randomized'].includes(settings.speed)) {
      return next(createValidationError('settings.speed deve ser: fast, normal, slow ou randomized'));
    }

    // Buscar instância (via helper que faz HTTP ao backend principal)
    const token = req.headers.authorization?.split(' ')[1];
    const instance = await getInstanceInfo(instanceId, token);
    
    // Buscar timezone do usuário do backend principal
    const { getUserTimezone } = await import('../utils/userHelper');
    const userTimezone = await getUserTimezone(userId, token);
    if (!instance) {
      return next(createNotFoundError('Instância'));
    }

    if (instance.status !== 'connected') {
      return next(createValidationError(`Instância "${instance.name}" não está conectada. Status atual: ${instance.status}`));
    }

    // Validar que temos um instanceName válido (não pode ser igual ao instanceId)
    if (!instance.instanceName || instance.instanceName === instanceId) {
      return next(createValidationError(`Não foi possível obter instanceName válido para a instância. Verifique se a instância existe e está configurada corretamente.`));
    }

    // Buscar template
    const template = await TemplateService.getById(templateId, userId);
    if (!template) {
      return next(createNotFoundError('Template'));
    }

    // Processar contatos
    let processedContacts: ContactData[] = [];

    if (contactsSource === 'kanban') {
      if (!columnIds || !Array.isArray(columnIds) || columnIds.length === 0) {
        return next(createValidationError('columnIds é obrigatório quando contactsSource é kanban'));
      }

      // Buscar contatos das colunas do PostgreSQL
      for (const columnId of columnIds) {
        const result = await pgPool.query(
          `SELECT phone, name, column_id FROM contacts WHERE user_id = $1 AND instance_id = $2 AND column_id = $3`,
          [userId, instanceId, columnId]
        );
        // Garantir que todos os números estejam normalizados
        result.rows.forEach((c: Record<string, any>) => {
          const normalized = ensureNormalizedPhone(c.phone);
          if (normalized) {
            processedContacts.push({
              phone: normalized,
              name: c.name,
              columnId: c.column_id || undefined,
            });
          }
        });
      }
    } else if (contactsSource === 'list') {
      if (!contactsData || !Array.isArray(contactsData)) {
        return next(createValidationError('contactsData é obrigatório quando contactsSource é list'));
      }
      processedContacts = contactsData;
    } else {
      return next(createValidationError('contactsSource inválido'));
    }

    if (processedContacts.length === 0) {
      return next(createValidationError('Nenhum contato fornecido'));
    }

    // Normalizar números - garantir que todos estejam normalizados
    const normalizedContacts = processedContacts
      .map((c) => {
        const normalized = ensureNormalizedPhone(c.phone);
        if (!normalized) {
          return null; // Ignorar números inválidos
        }
        return {
          ...c,
          phone: normalized,
        };
      })
      .filter((c): c is typeof processedContacts[0] & { phone: string } => c !== null);

    // Validar números
    let validatedContacts;
    try {
      validatedContacts = await validateContacts(instance.instanceName, normalizedContacts);
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
      console.warn(`⚠️ Validação de números não disponível (${errorMessage}). Usando contatos sem validação.`);
      validatedContacts = normalizedContacts.map((c) => ({
        phone: c.phone,
        name: c.name,
        validated: true,
        validationResult: undefined,
      }));
    }

    const validContacts = filterValidContacts(validatedContacts);

    if (validContacts.length === 0) {
      return next(createValidationError('Nenhum número válido encontrado após validação'));
    }

    const contactsDataForDispatch: ContactData[] = validContacts.map((c) => {
      // Garantir que o número está normalizado
      const normalizedPhone = ensureNormalizedPhone(c.phone) || c.phone;
      // Usar o número da validação se disponível (já vem normalizado), senão usar o normalizado
      const formattedPhone = c.validationResult?.number 
        ? ensureNormalizedPhone(c.validationResult.number) || c.validationResult.number
        : normalizedPhone;
      
      // Prioridade: nome fornecido (não vazio) > pushname da validação > undefined
      // O pushname (name da validação) será usado se não houver nome fornecido ou se o nome for vazio
      const providedName = c.name && c.name.trim() ? c.name.trim() : undefined;
      const finalName = providedName || c.validationResult?.name || undefined;
      
      return {
        phone: normalizedPhone,
        name: finalName,
        formattedPhone: formattedPhone,
      };
    });

    // Garantir que a data do schedule está no formato correto (YYYY-MM-DD)
    let normalizedSchedule = schedule;
    if (schedule && schedule.startDate) {
      // Se a data vier em formato diferente, normalizar para YYYY-MM-DD
      const dateStr = schedule.startDate;
      // Se já estiver no formato YYYY-MM-DD, usar diretamente
      if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        normalizedSchedule = { ...schedule, startDate: dateStr };
      } else {
        // Tentar converter de outros formatos
        const dateObj = new Date(dateStr);
        if (!isNaN(dateObj.getTime())) {
          const year = dateObj.getFullYear();
          const month = String(dateObj.getMonth() + 1).padStart(2, '0');
          const day = String(dateObj.getDate()).padStart(2, '0');
          normalizedSchedule = { ...schedule, startDate: `${year}-${month}-${day}` };
        }
      }
    }

    const dispatch = await DispatchService.create({
      userId,
      instanceId,
      instanceName: instance.instanceName, // Salvar instanceName ao criar
      templateId,
      name,
      settings,
      schedule: normalizedSchedule || null,
      contactsData: contactsDataForDispatch,
      defaultName: defaultName || null,
      userTimezone, // Salvar timezone do usuário
    });

    res.status(201).json({
      status: 'success',
      dispatch: {
        id: dispatch.id,
        name: dispatch.name,
        status: dispatch.status,
        stats: dispatch.stats,
        createdAt: dispatch.createdAt.toISOString(),
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao criar disparo'));
  }
};

export const uploadCSV = upload.single('file');

export const processCSVUpload = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    if (!req.file) {
      return next(createValidationError('Arquivo CSV é obrigatório'));
    }

    const contacts = await parseCSVFile(req.file.buffer);
    // Garantir que todos os números estejam normalizados
    const normalizedContacts = contacts
      .map((c) => {
        const normalized = ensureNormalizedPhone(c.phone);
        if (!normalized) {
          return null; // Ignorar números inválidos
        }
        return {
          ...c,
          phone: normalized,
        };
      })
      .filter((c): c is typeof contacts[0] & { phone: string } => c !== null);

    res.status(200).json({
      status: 'success',
      contacts: normalizedContacts,
      count: normalizedContacts.length,
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao processar CSV'));
  }
};

export const processInput = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { inputText } = req.body;

    if (!inputText || typeof inputText !== 'string') {
      return next(createValidationError('inputText é obrigatório'));
    }

    const contacts = parseInputText(inputText);
    // Garantir que todos os números estejam normalizados
    const normalizedContacts = contacts
      .map((c) => {
        const normalized = ensureNormalizedPhone(c.phone);
        if (!normalized) {
          return null; // Ignorar números inválidos
        }
        return {
          ...c,
          phone: normalized,
        };
      })
      .filter((c): c is typeof contacts[0] & { phone: string } => c !== null);

    res.status(200).json({
      status: 'success',
      contacts: normalizedContacts,
      count: normalizedContacts.length,
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao processar texto'));
  }
};

export const getDispatches = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const status = req.query.status as any;

    const dispatches = await DispatchService.getByUserId(userId, status);

    res.status(200).json({
      status: 'success',
      dispatches: dispatches.map((d) => ({
        id: d.id,
        name: d.name,
        status: d.status,
        instanceId: d.instanceId,
        templateId: d.templateId,
        settings: d.settings,
        schedule: d.schedule,
        stats: d.stats,
        defaultName: d.defaultName,
        createdAt: d.createdAt.toISOString(),
        startedAt: d.startedAt?.toISOString() || null,
        completedAt: d.completedAt?.toISOString() || null,
      })),
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao listar disparos'));
  }
};

export const getDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;

    const dispatch = await DispatchService.getById(id, userId);

    if (!dispatch) {
      return next(createNotFoundError('Disparo'));
    }

    res.status(200).json({
      status: 'success',
      dispatch: {
        id: dispatch.id,
        name: dispatch.name,
        status: dispatch.status,
        instanceId: dispatch.instanceId,
        templateId: dispatch.templateId,
        settings: dispatch.settings,
        schedule: dispatch.schedule,
        stats: dispatch.stats,
        defaultName: dispatch.defaultName,
        createdAt: dispatch.createdAt.toISOString(),
        startedAt: dispatch.startedAt?.toISOString() || null,
        completedAt: dispatch.completedAt?.toISOString() || null,
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao buscar disparo'));
  }
};

export const updateDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;
    const { name, settings, schedule, defaultName } = req.body;

    const dispatch = await DispatchService.getById(id, userId);
    if (!dispatch) {
      return next(createNotFoundError('Disparo'));
    }

    if (dispatch.status === 'running') {
      return next(createValidationError('Não é possível editar um disparo em execução. Pause o disparo primeiro.'));
    }

    if (dispatch.status === 'completed') {
      return next(createValidationError('Não é possível editar um disparo já concluído'));
    }

    const updateData: any = {};
    if (name !== undefined) updateData.name = name;
    if (settings !== undefined) updateData.settings = settings;
    if (schedule !== undefined) updateData.schedule = schedule;
    if (defaultName !== undefined) updateData.defaultName = defaultName;

    const updatedDispatch = await DispatchService.update(id, userId, updateData);

    if (!updatedDispatch) {
      return next(createNotFoundError('Disparo'));
    }

    res.status(200).json({
      status: 'success',
      dispatch: {
        id: updatedDispatch.id,
        name: updatedDispatch.name,
        status: updatedDispatch.status,
        instanceId: updatedDispatch.instanceId,
        templateId: updatedDispatch.templateId,
        settings: updatedDispatch.settings,
        schedule: updatedDispatch.schedule,
        stats: updatedDispatch.stats,
        defaultName: updatedDispatch.defaultName,
        createdAt: updatedDispatch.createdAt.toISOString(),
        startedAt: updatedDispatch.startedAt?.toISOString() || null,
        completedAt: updatedDispatch.completedAt?.toISOString() || null,
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao atualizar disparo'));
  }
};

export const deleteDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;

    const deleted = await DispatchService.delete(id, userId);

    if (!deleted) {
      return next(createNotFoundError('Disparo'));
    }

    res.status(200).json({
      status: 'success',
      message: 'Disparo deletado com sucesso',
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao deletar disparo'));
  }
};

export const startDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;

    const dispatch = await DispatchService.getById(id, userId);
    if (!dispatch) {
      return next(createNotFoundError('Disparo'));
    }

    if (dispatch.status === 'running') {
      return next(createValidationError('Disparo já está em execução'));
    }

    if (dispatch.status === 'completed') {
      return next(createValidationError('Disparo já foi concluído'));
    }

    // Sempre mudar para 'running' quando o usuário clicar em Start
    // O scheduler verificará se já passou a hora agendada antes de processar
    await DispatchService.update(id, userId, { 
      status: 'running',
      startedAt: new Date(),
    });

    res.status(200).json({
      status: 'success',
      message: 'Disparo iniciado com sucesso',
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao iniciar disparo'));
  }
};

export const pauseDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;

    const dispatch = await DispatchService.update(id, userId, { status: 'paused' });

    if (!dispatch) {
      return next(createNotFoundError('Disparo'));
    }

    res.status(200).json({
      status: 'success',
      message: 'Disparo pausado com sucesso',
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao pausar disparo'));
  }
};

export const resumeDispatch = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { id } = req.params;

    const dispatch = await DispatchService.getById(id, userId);
    if (!dispatch) {
      return next(createNotFoundError('Disparo'));
    }

    // O scheduler processará o disparo automaticamente quando o status for 'running'
    await DispatchService.update(id, userId, { status: 'running' });

    res.status(200).json({
      status: 'success',
      message: 'Disparo retomado com sucesso',
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao retomar disparo'));
  }
};

export const validateContactsNumbers = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { instanceId, contacts } = req.body;

    if (!instanceId) {
      return next(createValidationError('ID da instância é obrigatório'));
    }

    if (!contacts || !Array.isArray(contacts) || contacts.length === 0) {
      return next(createValidationError('Lista de contatos é obrigatória'));
    }

    const instance = await getInstanceInfo(instanceId, req.headers.authorization?.split(' ')[1]);
    if (!instance) {
      return next(createValidationError('Instância não encontrada'));
    }

    // Garantir que todos os números estejam normalizados antes de validar
    const normalizedContacts = contacts
      .map((c) => {
        const normalized = ensureNormalizedPhone(c.phone);
        if (!normalized) {
          return null; // Ignorar números inválidos
        }
        return {
          ...c,
          phone: normalized,
        };
      })
      .filter((c): c is typeof contacts[0] & { phone: string } => c !== null);

    if (normalizedContacts.length === 0) {
      return next(createValidationError('Nenhum número válido encontrado após normalização'));
    }

    const validatedContacts = await validateContacts(instance.instanceName, normalizedContacts);

    res.status(200).json({
      status: 'success',
      contacts: validatedContacts.map((c) => ({
        phone: c.phone,
        name: c.name,
        validated: c.validated,
        validationResult: c.validationResult
          ? {
              exists: c.validationResult.exists,
              name: c.validationResult.name,
            }
          : null,
      })),
      stats: {
        total: validatedContacts.length,
        valid: validatedContacts.filter((c) => c.validated).length,
        invalid: validatedContacts.filter((c) => !c.validated).length,
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao validar contatos'));
  }
};

