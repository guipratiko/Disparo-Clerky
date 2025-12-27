import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { AppError } from './errorHandler';
import { JWT_CONFIG } from '../config/constants';

// Interface para adicionar user ao Request
export interface AuthRequest extends Request {
  user?: {
    id: string;
  };
}

export const protect = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
) => {
  try {
    let token: string | undefined;

    // Verificar se o token está no header
    if (req.headers.authorization && req.headers.authorization.startsWith('Bearer')) {
      token = req.headers.authorization.split(' ')[1];
    }

    if (!token) {
      const error: AppError = new Error('Token não fornecido. Faça login para acessar.');
      error.statusCode = 401;
      error.status = 'unauthorized';
      return next(error);
    }

    // Verificar token (mesmo secret do backend principal)
    const decoded = jwt.verify(token, JWT_CONFIG.SECRET) as { id: string };

    // Adicionar user ao request
    req.user = {
      id: decoded.id,
    };

    next();
  } catch (error: unknown) {
    if (error instanceof Error) {
      if (error.name === 'JsonWebTokenError') {
        const jwtError: AppError = new Error('Token inválido');
        jwtError.statusCode = 401;
        jwtError.status = 'unauthorized';
        return next(jwtError);
      }
      if (error.name === 'TokenExpiredError') {
        const expiredError: AppError = new Error('Token expirado');
        expiredError.statusCode = 401;
        expiredError.status = 'unauthorized';
        return next(expiredError);
      }
    }
    const appError: AppError = new Error('Erro ao verificar token');
    appError.statusCode = 401;
    appError.status = 'unauthorized';
    return next(appError);
  }
};

