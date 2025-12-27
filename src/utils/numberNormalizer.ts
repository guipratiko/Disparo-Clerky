/**
 * Utilitário para normalização de números de telefone
 */

/**
 * Remove todos os caracteres não numéricos de uma string
 */
const removeNonNumeric = (value: string): string => {
  return value.replace(/\D/g, '');
};

/**
 * Normaliza um número de telefone para formato internacional
 */
export const normalizePhone = (phone: string, defaultDDI: string = '55'): string | null => {
  if (!phone || typeof phone !== 'string') {
    return null;
  }

  const digitsOnly = removeNonNumeric(phone);

  if (digitsOnly.length === 0) {
    return null;
  }

  let normalized = digitsOnly.startsWith('0') ? digitsOnly.substring(1) : digitsOnly;

  if (normalized.startsWith('55') && (normalized.length === 12 || normalized.length === 13)) {
    return normalized;
  }

  if (normalized.length === 10 || normalized.length === 11) {
    return `${defaultDDI}${normalized}`;
  }

  if (normalized.length === 12 || normalized.length === 13) {
    const firstTwo = normalized.substring(0, 2);
    const validDDDs = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99'];
    if (validDDDs.includes(firstTwo)) {
      return `${defaultDDI}${normalized}`;
    }
    return normalized;
  }

  if (normalized.length > 13) {
    return normalized;
  }

  if (normalized.length < 10) {
    return null;
  }

  return `${defaultDDI}${normalized}`;
};

/**
 * Normaliza uma lista de números de telefone
 */
export const normalizePhoneList = (
  phones: string[],
  defaultDDI: string = '55'
): string[] => {
  return phones
    .map((phone) => normalizePhone(phone, defaultDDI))
    .filter((phone): phone is string => phone !== null);
};

/**
 * Formata número para exibição (ex: (62) 99844-8536)
 */
export const formatPhoneForDisplay = (phone: string): string => {
  if (!phone) return '';
  
  const digitsOnly = removeNonNumeric(phone);
  
  if (digitsOnly.length === 13 && digitsOnly.startsWith('55')) {
    // Formato: 55 + DDD (2) + número (9 ou 8 dígitos)
    const ddd = digitsOnly.substring(2, 4);
    const number = digitsOnly.substring(4);
    
    if (number.length === 9) {
      // Celular: (DDD) 9XXXX-XXXX
      return `(${ddd}) ${number.substring(0, 5)}-${number.substring(5)}`;
    } else if (number.length === 8) {
      // Fixo: (DDD) XXXX-XXXX
      return `(${ddd}) ${number.substring(0, 4)}-${number.substring(4)}`;
    }
  }
  
  return phone;
};

/**
 * Garante que um número está normalizado
 * Remove @s.whatsapp.net se presente e normaliza
 */
export const ensureNormalizedPhone = (phone: string, defaultDDI: string = '55'): string | null => {
  if (!phone || typeof phone !== 'string') {
    return null;
  }

  // Remover @s.whatsapp.net se presente
  let cleanPhone = phone.replace('@s.whatsapp.net', '').trim();

  // Normalizar
  return normalizePhone(cleanPhone, defaultDDI);
};

/**
 * Garante que uma lista de números está normalizada
 */
export const ensureNormalizedPhoneList = (
  phones: string[],
  defaultDDI: string = '55'
): string[] => {
  return phones
    .map((phone) => ensureNormalizedPhone(phone, defaultDDI))
    .filter((phone): phone is string => phone !== null);
};

