/**
 * Helper para buscar informações do usuário do backend principal
 */

import axios from 'axios';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:4331';

/**
 * Buscar timezone do usuário do backend principal
 */
export const getUserTimezone = async (userId: string, token?: string): Promise<string> => {
  try {
    const response = await axios.get(`${BACKEND_URL}/api/auth/me`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      timeout: 5000,
    });

    if (response.data && response.data.user && response.data.user.timezone) {
      return response.data.user.timezone;
    }
  } catch (error) {
    // Silenciar erro - usar padrão
  }

  return 'America/Sao_Paulo'; // Default
};

