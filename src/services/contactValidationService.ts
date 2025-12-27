/**
 * Serviço para validação de números de WhatsApp
 * Utiliza a Evolution API para verificar se um número existe no WhatsApp
 */

import { requestEvolutionAPI } from '../utils/evolutionAPI';
import { ensureNormalizedPhone } from '../utils/numberNormalizer';

export interface ValidationResult {
  jid: string;
  exists: boolean;
  number: string;
  name?: string;
  lid?: string;
}

export interface ContactValidationData {
  phone: string;
  name?: string;
  validated?: boolean;
  validationResult?: ValidationResult;
}

export const validatePhoneNumbers = async (
  instanceName: string,
  phones: string[]
): Promise<ValidationResult[]> => {
  try {
    // Garantir que todos os números estejam normalizados
    const normalizedPhones = phones
      .map((phone) => ensureNormalizedPhone(phone))
      .filter((phone): phone is string => phone !== null);

    if (normalizedPhones.length === 0) {
      return [];
    }

    let response;
    let endpointAvailable = false;

    try {
      response = await requestEvolutionAPI(
        'POST',
        `/chat/whatsappNumbers/${encodeURIComponent(instanceName)}`,
        { numbers: normalizedPhones }
      );
      endpointAvailable = true;
    } catch (error) {
      try {
        response = await requestEvolutionAPI(
          'POST',
          `/chat/whatsappNumbers/${encodeURIComponent(instanceName)}`,
          { numbers: normalizedPhones }
        );
        endpointAvailable = true;
      } catch {
        return [];
      }
    }

    if (!endpointAvailable || !response) {
      return [];
    }

    // requestEvolutionAPI retorna { statusCode, data }
    // A resposta da API pode vir como array direto em response.data
    const responseData = response.data || response;
    
    const results: ValidationResult[] = [];
    if (Array.isArray(responseData)) {
      for (const item of responseData) {
        if (item && item.number) {
          results.push({
            jid: item.jid || `${item.number}@s.whatsapp.net`,
            exists: item.exists !== false,
            number: item.number,
            name: item.name, // Pushname do WhatsApp
            lid: item.lid,
          });
        }
      }
    }

    return results;
  } catch (error) {
    console.error('Erro ao validar números:', error);
    return [];
  }
};

export const validateContacts = async (
  instanceName: string,
  contacts: Array<{ phone: string; name?: string }>
): Promise<ContactValidationData[]> => {
  // Garantir que todos os números estejam normalizados
  const uniquePhones = Array.from(
    new Set(contacts.map((c) => ensureNormalizedPhone(c.phone)).filter(Boolean) as string[])
  );

  const validationResults = await validatePhoneNumbers(instanceName, uniquePhones);
  const validationAvailable = validationResults.length > 0;

  const resultsMap = new Map<string, ValidationResult>();
  for (const result of validationResults) {
    if (result.exists) {
      resultsMap.set(result.number, result);
    }
  }

  const validatedContacts: ContactValidationData[] = [];

  for (const contact of contacts) {
    // Garantir que o número está normalizado
    const normalizedPhone = ensureNormalizedPhone(contact.phone);
    if (!normalizedPhone) {
      continue;
    }

    const validationResult = resultsMap.get(normalizedPhone);

    if (validationAvailable) {
      if (validationResult && validationResult.exists) {
        // Prioridade: nome fornecido (não vazio) > pushname da validação > undefined
        const providedName = contact.name && contact.name.trim() ? contact.name.trim() : undefined;
        const finalName = providedName || validationResult.name || undefined;
        validatedContacts.push({
          phone: normalizedPhone,
          name: finalName,
          validated: true,
          validationResult,
        });
      } else {
        validatedContacts.push({
          phone: normalizedPhone,
          name: contact.name,
          validated: false,
        });
      }
    } else {
      validatedContacts.push({
        phone: normalizedPhone,
        name: contact.name,
        validated: true,
      });
    }
  }

  return validatedContacts;
};

export const filterValidContacts = (
  contacts: ContactValidationData[]
): ContactValidationData[] => {
  return contacts.filter((c) => c.validated === true);
};

