/**
 * Utilitário para trabalhar com timezones em agendamentos
 */

import { DispatchSchedule } from '../types/dispatch';
import axios from 'axios';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:4331';

/**
 * Obter timezone do usuário do backend principal
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
    console.warn(`⚠️  Não foi possível obter timezone do usuário ${userId}, usando padrão`);
  }

  return 'America/Sao_Paulo'; // Default
};

/**
 * Obter timezone a ser usado para um agendamento
 * Prioridade: timezone do schedule > timezone do perfil > padrão
 */
export const getScheduleTimezone = (
  schedule: DispatchSchedule | null,
  userTimezone: string
): string => {
  if (schedule?.timezone) {
    return schedule.timezone;
  }
  return userTimezone || 'America/Sao_Paulo';
};

/**
 * Converter data/hora local para UTC considerando o timezone
 * Usa uma abordagem mais simples e confiável
 */
export const convertToUTC = (
  dateStr: string, // YYYY-MM-DD
  timeStr: string, // HH:mm
  timezone: string
): Date => {
  // Criar uma data assumindo que os valores estão no timezone local do servidor
  const [year, month, day] = dateStr.split('-').map(Number);
  const [hour, minute] = timeStr.split(':').map(Number);
  
  // Criar data local (assumindo timezone do servidor)
  const localDate = new Date(year, month - 1, day, hour, minute, 0, 0);
  
  // Obter a mesma data/hora no timezone especificado
  const tzDateStr = localDate.toLocaleString('en-US', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  
  // Parse da string formatada
  const parts = tzDateStr.split(/[/,\s:]/);
  const tzMonth = parseInt(parts[0], 10);
  const tzDay = parseInt(parts[1], 10);
  const tzYear = parseInt(parts[2], 10);
  const tzHour = parseInt(parts[3], 10);
  const tzMinute = parseInt(parts[4], 10);
  
  // Criar data UTC equivalente
  const tzDateUTC = new Date(Date.UTC(tzYear, tzMonth - 1, tzDay, tzHour, tzMinute, 0));
  
  // Calcular offset: diferença entre o que queremos e o que temos
  const desiredUTC = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
  const offset = desiredUTC.getTime() - tzDateUTC.getTime();
  
  return new Date(desiredUTC.getTime() - offset);
};

/**
 * Verificar se a data/hora de início já passou considerando o timezone
 */
export const hasStartDatePassed = (
  schedule: DispatchSchedule | null,
  userTimezone: string
): boolean => {
  if (!schedule || !schedule.startDate) {
    return true; // Sem data de início, pode iniciar imediatamente
  }

  const timezone = getScheduleTimezone(schedule, userTimezone);
  const now = new Date();
  
  // Obter data/hora atual no timezone especificado
  const nowDateStr = now.toLocaleDateString('en-CA', { timeZone: timezone }); // YYYY-MM-DD
  const nowTimeStr = now.toLocaleTimeString('en-US', { 
    timeZone: timezone, 
    hour: '2-digit', 
    minute: '2-digit', 
    hour12: false 
  });
  
  // Comparar data
  if (schedule.startDate > nowDateStr) {
    return false; // Data ainda não chegou
  }
  
  if (schedule.startDate < nowDateStr) {
    return true; // Data já passou
  }
  
  // Mesma data, comparar hora
  return nowTimeStr >= schedule.startTime;
};

/**
 * Verificar se está dentro do horário permitido considerando o timezone
 */
export const isWithinAllowedHours = (
  schedule: DispatchSchedule,
  userTimezone: string
): boolean => {
  const timezone = getScheduleTimezone(schedule, userTimezone);
  const now = new Date();
  
  // Obter hora atual no timezone especificado (formato HH:mm)
  const currentTimeStr = now.toLocaleTimeString('en-US', {
    timeZone: timezone,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  
  // Comparar strings no formato HH:mm (funciona porque é formato lexicográfico)
  return currentTimeStr >= schedule.startTime && currentTimeStr <= schedule.endTime;
};

