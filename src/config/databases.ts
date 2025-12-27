/**
 * Configuração e gerenciamento de conexões de banco de dados
 * - PostgreSQL: Templates, Dispatches
 */

import { Pool, PoolClient } from 'pg';
import { POSTGRES_CONFIG } from './constants';

// ============================================
// PostgreSQL (Templates, Dispatches)
// ============================================
export const pgPool = new Pool({
  connectionString: POSTGRES_CONFIG.URI,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Event listeners para PostgreSQL (sem log para evitar spam)

pgPool.on('error', (err) => {
  console.error('❌ Erro inesperado no pool PostgreSQL:', err);
});

// Função para testar conexão PostgreSQL
export const testPostgreSQL = async (): Promise<boolean> => {
  try {
    const client = await pgPool.connect();
    await client.query('SELECT NOW()');
    client.release();
    return true;
  } catch (error) {
    console.error('❌ Erro ao testar conexão PostgreSQL:', error);
    return false;
  }
};

// Função para obter cliente PostgreSQL (para transações)
export const getPostgreSQLClient = async (): Promise<PoolClient> => {
  return await pgPool.connect();
};

// ============================================
// Função para conectar todos os bancos
// ============================================
export const connectAllDatabases = async (): Promise<void> => {
  try {
    // Testar PostgreSQL
    const pgConnected = await testPostgreSQL();
    if (pgConnected) {
      console.log('✅ PostgreSQL conectado e testado');
    } else {
      console.warn('⚠️  PostgreSQL não conectado, mas continuando...');
    }
  } catch (error) {
    console.error('❌ Erro ao conectar bancos de dados:', error);
    throw error;
  }
};

// ============================================
// Função para fechar todas as conexões
// ============================================
export const closeAllDatabases = async (): Promise<void> => {
  try {
    // Fechar PostgreSQL
    await pgPool.end();
    console.log('✅ PostgreSQL desconectado');
  } catch (error) {
    console.error('❌ Erro ao fechar conexões:', error);
  }
};

