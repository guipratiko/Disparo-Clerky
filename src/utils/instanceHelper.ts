/**
 * Helper para buscar informações de instâncias do backend principal
 * Como o microserviço não tem acesso ao MongoDB, fazemos chamadas HTTP
 */

import axios from 'axios';
import { SERVER_CONFIG } from '../config/constants';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:4331';

export interface InstanceInfo {
  _id: string;
  instanceName: string;
  status: string;
  name: string;
}

/**
 * Buscar instância do backend principal
 * Nota: Requer que o backend principal tenha um endpoint para buscar instâncias
 * Por enquanto, retorna uma estrutura básica
 */
export const getInstanceInfo = async (
  instanceId: string,
  token?: string
): Promise<InstanceInfo | null> => {
  try {
    // Tentar buscar do backend principal via HTTP
    // Se não funcionar, retornar estrutura básica
    const response = await axios.get(`${BACKEND_URL}/api/instances/${instanceId}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      timeout: 5000,
    });

    if (response.data && response.data.instance) {
      return {
        _id: response.data.instance._id || instanceId,
        instanceName: response.data.instance.instanceName || instanceId,
        status: response.data.instance.status || 'unknown',
        name: response.data.instance.name || 'Instância',
      };
    }

    return null;
  } catch (error: any) {
    // Se falhar, não retornar estrutura básica com instanceId como instanceName
    // Isso causaria erro na Evolution API
    const errorMessage = error?.response?.data?.message || error?.message || 'Erro desconhecido';
    console.warn(`⚠️ Não foi possível buscar instância ${instanceId} do backend principal: ${errorMessage}`);
    
    // Retornar null para que o código chamador possa tratar o erro
    return null;
  }
};

