/**
 * Controller para Templates de Disparos
 */

import { Response, NextFunction } from 'express';
import { AuthRequest } from '../middleware/auth';
import {
  createValidationError,
  createNotFoundError,
  handleControllerError,
} from '../utils/errorHelpers';
import { TemplateService, TemplateType } from '../services/templateService';
import { uploadFileToService } from '../utils/mediaService';
import multer from 'multer';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB
  },
});

export const uploadTemplateFile = upload.single('file');

export const createTemplate = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const { name, type, content } = req.body;

    if (!name || !type || !content) {
      return next(createValidationError('Nome, tipo e conteúdo são obrigatórios'));
    }

    const validTypes: TemplateType[] = ['text', 'image', 'image_caption', 'video', 'video_caption', 'audio', 'file', 'sequence'];
    const sanitizedType = (type || '').toLowerCase().trim() as TemplateType;

    if (!validTypes.includes(sanitizedType)) {
      return next(createValidationError(`Tipo inválido. Tipos válidos: ${validTypes.join(', ')}`));
    }

    if (sanitizedType === 'sequence') {
      const validation = TemplateService.validateSequenceContent(content);
      if (!validation.valid) {
        return next(createValidationError(validation.error || 'Conteúdo de sequência inválido'));
      }
    }

    const template = await TemplateService.create({
      userId,
      name,
      type: sanitizedType,
      content,
    });

    res.status(201).json({
      status: 'success',
      template: {
        id: template.id,
        name: template.name,
        type: template.type,
        content: template.content,
        createdAt: template.createdAt.toISOString(),
        updatedAt: template.updatedAt.toISOString(),
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao criar template'));
  }
};

export const getTemplates = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return next(createValidationError('Usuário não autenticado'));
    }

    const type = req.query.type as TemplateType | undefined;

    const templates = await TemplateService.getByUserId(userId, type);

    res.status(200).json({
      status: 'success',
      templates: templates.map((t) => ({
        id: t.id,
        name: t.name,
        type: t.type,
        content: t.content,
        createdAt: t.createdAt.toISOString(),
        updatedAt: t.updatedAt.toISOString(),
      })),
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao listar templates'));
  }
};

export const getTemplate = async (
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

    const template = await TemplateService.getById(id, userId);

    if (!template) {
      return next(createNotFoundError('Template'));
    }

    res.status(200).json({
      status: 'success',
      template: {
        id: template.id,
        name: template.name,
        type: template.type,
        content: template.content,
        createdAt: template.createdAt.toISOString(),
        updatedAt: template.updatedAt.toISOString(),
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao buscar template'));
  }
};

export const updateTemplate = async (
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
    const { name, content } = req.body;

    const currentTemplate = await TemplateService.getById(id, userId);
    if (!currentTemplate) {
      return next(createNotFoundError('Template'));
    }

    if (content && currentTemplate.type === 'sequence') {
      const validation = TemplateService.validateSequenceContent(content);
      if (!validation.valid) {
        return next(createValidationError(validation.error || 'Conteúdo de sequência inválido'));
      }
    }

    const template = await TemplateService.update(id, userId, { name, content });

    if (!template) {
      return next(createNotFoundError('Template'));
    }

    res.status(200).json({
      status: 'success',
      template: {
        id: template.id,
        name: template.name,
        type: template.type,
        content: template.content,
        createdAt: template.createdAt.toISOString(),
        updatedAt: template.updatedAt.toISOString(),
      },
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao atualizar template'));
  }
};

export const deleteTemplate = async (
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

    const deleted = await TemplateService.delete(id, userId);

    if (!deleted) {
      return next(createNotFoundError('Template'));
    }

    res.status(200).json({
      status: 'success',
      message: 'Template deletado com sucesso',
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao deletar template'));
  }
};

export const uploadTemplateFileHandler = async (
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
      return next(createValidationError('Arquivo é obrigatório'));
    }

    const fileName = req.file.originalname || `file-${Date.now()}`;
    const uploadResult = await uploadFileToService(
      req.file.buffer,
      fileName,
      req.file.mimetype
    );

    if (!uploadResult) {
      return next(createValidationError('Erro ao fazer upload do arquivo'));
    }

    res.status(200).json({
      status: 'success',
      url: uploadResult.url,
      fullUrl: uploadResult.fullUrl,
      fileName: uploadResult.fileName,
    });
  } catch (error: unknown) {
    return next(handleControllerError(error, 'Erro ao fazer upload do arquivo'));
  }
};

