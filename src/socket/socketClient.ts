/**
 * Cliente Socket.io para integrar com o backend principal
 * Emite eventos de atualização de disparos para o frontend via backend principal
 */

import { io, Socket } from 'socket.io-client';
import { SOCKET_CONFIG } from '../config/constants';
import { Dispatch } from '../types/dispatch';

let socket: Socket | null = null;
let isConnected = false;
let hasWarnedAboutConnection = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 3;

/**
 * Conectar ao servidor Socket.io do backend principal
 * Nota: O Socket.io é opcional - se não conectar, o sistema continua funcionando
 * mas não emite atualizações em tempo real para o frontend
 */
export const connectSocket = (): void => {
  if (socket && isConnected) {
    return;
  }

  // Se já tentou muitas vezes, não tentar mais
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    if (!hasWarnedAboutConnection) {
      console.warn('⚠️ Socket.io: Muitas tentativas de reconexão falharam. Desabilitando reconexão automática.');
      hasWarnedAboutConnection = true;
    }
    return;
  }

  try {
    // Socket.io é opcional - não requer autenticação para o microserviço
    // O backend principal pode aceitar conexões sem autenticação para eventos internos
    socket = io(SOCKET_CONFIG.URL, {
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 2000,
      reconnectionDelayMax: 10000,
      reconnectionAttempts: MAX_RECONNECT_ATTEMPTS,
      timeout: 5000,
      // Não enviar token - o backend principal pode ter um endpoint especial para microserviços
      // ou aceitar conexões sem autenticação para eventos internos
    });

    socket.on('connect', () => {
      isConnected = true;
      reconnectAttempts = 0; // Resetar contador ao conectar com sucesso
      hasWarnedAboutConnection = false;
      console.log('✅ Conectado ao Socket.io do backend principal');
    });

    socket.on('disconnect', (reason) => {
      isConnected = false;
      // Não logar desconexões normais (como "transport close")
      if (reason !== 'transport close' && reason !== 'io client disconnect') {
        console.log(`❌ Desconectado do Socket.io: ${reason}`);
      }
    });

    socket.on('connect_error', (error) => {
      reconnectAttempts++;
      // Socket.io é opcional - apenas logar uma vez
      if (!hasWarnedAboutConnection) {
        if (error.message.includes('Token')) {
          console.warn('⚠️ Socket.io requer autenticação. Atualizações em tempo real podem não funcionar.');
        } else {
          console.warn('⚠️ Erro ao conectar ao Socket.io:', error.message);
        }
        hasWarnedAboutConnection = true;
      }
    });

    socket.on('reconnect_attempt', (attemptNumber) => {
      if (attemptNumber >= MAX_RECONNECT_ATTEMPTS) {
        console.warn(`⚠️ Socket.io: Limite de tentativas de reconexão atingido (${MAX_RECONNECT_ATTEMPTS}).`);
      }
    });
  } catch (error) {
    reconnectAttempts++;
    console.error('❌ Erro ao inicializar Socket.io:', error);
  }
};

/**
 * Emitir evento de atualização de disparo
 * O backend principal irá re-emitir para o frontend
 */
export const emitDispatchUpdate = (userId: string, dispatch: Dispatch): void => {
  if (!socket || !isConnected) {
    // Tentar conectar apenas uma vez
    if (!socket) {
      connectSocket();
    }
    return;
  }

  socket.emit('dispatch-updated', {
    userId,
    dispatch: {
      id: dispatch.id,
      name: dispatch.name,
      status: dispatch.status,
      stats: dispatch.stats,
      settings: dispatch.settings,
      schedule: dispatch.schedule,
      defaultName: dispatch.defaultName,
      createdAt: dispatch.createdAt instanceof Date ? dispatch.createdAt.toISOString() : dispatch.createdAt,
      startedAt: dispatch.startedAt instanceof Date ? dispatch.startedAt.toISOString() : dispatch.startedAt,
      completedAt: dispatch.completedAt instanceof Date ? dispatch.completedAt.toISOString() : dispatch.completedAt,
      updatedAt: dispatch.updatedAt instanceof Date ? dispatch.updatedAt.toISOString() : dispatch.updatedAt,
    },
  });
};

/**
 * Desconectar do Socket.io
 */
export const disconnectSocket = (): void => {
  if (socket) {
    socket.disconnect();
    socket = null;
    isConnected = false;
    console.log('✅ Desconectado do Socket.io');
  }
};

