/**
 * Utilitário para substituição de variáveis dinâmicas em templates
 */

import { formatPhoneForDisplay } from './numberNormalizer';
import { ContactData } from '../types/dispatch';

const getFirstName = (fullName?: string): string => {
  if (!fullName || !fullName.trim()) {
    return '';
  }
  const parts = fullName.trim().split(/\s+/);
  return parts[0] || '';
};

const getLastName = (fullName?: string): string => {
  if (!fullName || !fullName.trim()) {
    return '';
  }
  const parts = fullName.trim().split(/\s+/);
  if (parts.length <= 1) {
    return '';
  }
  return parts.slice(1).join(' ') || '';
};

export const replaceVariables = (
  text: string,
  contact: ContactData,
  defaultName: string = 'Cliente',
  typebotVariables?: Record<string, unknown>
): string => {
  if (!text || typeof text !== 'string') {
    return text;
  }

  // Usar o nome do contato se existir e não for vazio, senão usar defaultName
  const contactName = (contact.name && contact.name.trim()) ? contact.name.trim() : defaultName;
  const firstName = getFirstName(contactName);
  const lastName = getLastName(contactName);
  const fullName = contactName;
  const formattedPhone = contact.formattedPhone || formatPhoneForDisplay(contact.phone);
  const originalPhone = contact.phone;

  const variables: Record<string, string> = {
    $name: fullName, // Alias para $fullName (nome completo)
    $firstName: firstName,
    $lastName: lastName,
    $fullName: fullName,
    $formattedPhone: formattedPhone,
    $originalPhone: originalPhone,
  };

  if (typebotVariables && typeof typebotVariables === 'object') {
    for (const [key, value] of Object.entries(typebotVariables)) {
      const variableKey = `$${key}`;
      variables[variableKey] = value != null ? String(value) : '';
    }
  }

  let result = text;
  for (const [variable, value] of Object.entries(variables)) {
    const regex = new RegExp(variable.replace(/\$/g, '\\$'), 'g');
    result = result.replace(regex, value);
  }

  return result;
};

import { TemplateContent } from '../types/dispatch';

export const replaceVariablesInContent = (
  content: TemplateContent | string | unknown,
  contact: ContactData,
  defaultName: string = 'Cliente'
): TemplateContent | string | unknown => {
  if (typeof content === 'string') {
    return replaceVariables(content, contact, defaultName);
  }

  if (Array.isArray(content)) {
    return content.map((item) => replaceVariablesInContent(item, contact, defaultName));
  }

  if (content && typeof content === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(content)) {
      result[key] = replaceVariablesInContent(value, contact, defaultName);
    }
    return result;
  }

  return content;
};

export const AVAILABLE_VARIABLES = [
  { variable: '$name', label: 'Nome', description: 'Nome completo do contato (alias para $fullName)' },
  { variable: '$firstName', label: 'Primeiro Nome', description: 'Primeiro nome do contato' },
  { variable: '$lastName', label: 'Último Nome', description: 'Último nome do contato' },
  { variable: '$fullName', label: 'Nome Completo', description: 'Nome completo do contato' },
  { variable: '$formattedPhone', label: 'Número Formatado', description: 'Número formatado (ex: (62) 99844-8536)' },
  { variable: '$originalPhone', label: 'Número Original', description: 'Número original/normalizado' },
];

