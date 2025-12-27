/**
 * Configurações centralizadas do microserviço de Disparos
 */

import dotenv from 'dotenv';

// Carregar variáveis de ambiente
dotenv.config();

// Server Configuration
export const SERVER_CONFIG = {
  PORT: parseInt(process.env.PORT || '4332', 10),
  NODE_ENV: process.env.NODE_ENV || 'development',
  CORS_ORIGIN: process.env.CORS_ORIGIN || 'http://localhost:3000',
};

// JWT Configuration (mesmo secret do backend principal)
export const JWT_CONFIG = {
  SECRET: process.env.JWT_SECRET || 'your-secret-key-change-in-production',
  EXPIRE: process.env.JWT_EXPIRE || '7d',
};

// PostgreSQL Configuration (mesmo banco do backend principal)
export const POSTGRES_CONFIG = {
  URI: process.env.POSTGRES_URI || 'postgres://user:password@localhost:5432/clerky_db',
};

// Socket.io Configuration (backend principal)
export const SOCKET_CONFIG = {
  URL: process.env.SOCKET_URL || 'http://localhost:4331',
};

// Evolution API Configuration
export const EVOLUTION_CONFIG = {
  HOST: process.env.EVOLUTION_HOST || 'evo.clerky.com.br',
  API_KEY: process.env.EVOLUTION_APIKEY || process.env.EVOLUTION_API_KEY || '',
  URL: process.env.EVOLUTION_API_URL || 'https://evo.clerky.com.br',
};

// Media Service Configuration
export const MEDIA_SERVICE_CONFIG = {
  URL: process.env.MEDIA_SERVICE_URL || 'http://localhost:3001',
  TOKEN: process.env.MEDIA_SERVICE_TOKEN || '',
};

