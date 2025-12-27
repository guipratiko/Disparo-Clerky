/**
 * Utilitário para parsing de arquivos CSV
 */

export interface ParsedContact {
  phone: string;
  name?: string;
}

const parseCSVLine = (line: string): ParsedContact | null => {
  if (!line || !line.trim()) {
    return null;
  }

  const parts = line
    .trim()
    .split(/[,;]/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);

  if (parts.length === 0) {
    return null;
  }

  if (parts.length === 1) {
    return {
      phone: parts[0],
    };
  }

  return {
    name: parts[0],
    phone: parts[1],
  };
};

export const parseCSVText = (csvText: string): ParsedContact[] => {
  if (!csvText || !csvText.trim()) {
    return [];
  }

  const lines = csvText.split(/\r?\n/);
  const contacts: ParsedContact[] = [];
  
  for (const line of lines) {
    const contact = parseCSVLine(line);
    if (contact) {
      contacts.push(contact);
    }
  }

  return contacts;
};

export const parseCSVFile = async (fileBuffer: Buffer): Promise<ParsedContact[]> => {
  const csvText = fileBuffer.toString('utf-8');
  return parseCSVText(csvText);
};

export const parseInputText = (inputText: string): ParsedContact[] => {
  if (!inputText || !inputText.trim()) {
    return [];
  }

  const lines = inputText.split(/\r?\n/);
  const contacts: ParsedContact[] = [];
  
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (trimmed.includes(';')) {
      const parts = trimmed.split(';').map((p) => p.trim());
      if (parts.length >= 2) {
        contacts.push({
          name: parts[0],
          phone: parts[1],
        });
      } else if (parts.length === 1) {
        contacts.push({
          phone: parts[0],
        });
      }
    } else {
      contacts.push({
        phone: trimmed,
      });
    }
  }

  return contacts;
};

