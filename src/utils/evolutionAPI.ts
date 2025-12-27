/**
 * Helper para fazer requisições para Evolution API
 */

import { https, http } from 'follow-redirects';
import { EVOLUTION_CONFIG } from '../config/constants';

export const requestEvolutionAPI = async (
  method: string,
  path: string,
  body?: any
): Promise<{ statusCode: number; data: any }> => {
  const hostname = EVOLUTION_CONFIG.HOST;
  const apiKey = EVOLUTION_CONFIG.API_KEY;

  if (!apiKey) {
    throw new Error('EVOLUTION_API_KEY não configurada no .env');
  }

  return new Promise((resolve, reject) => {
    const data = body ? Buffer.from(JSON.stringify(body), 'utf8') : null;
    const isHttps = EVOLUTION_CONFIG.URL.startsWith('https');
    const requestModule = isHttps ? https : http;

    const options = {
      hostname,
      method,
      path,
      headers: {
        apikey: apiKey,
        ...(body
          ? {
              'Content-Type': 'application/json',
              'Content-Length': data!.length,
            }
          : {}),
      },
      maxRedirects: 20,
    };

    const req = requestModule.request(options, (res: any) => {
      const chunks: Buffer[] = [];

      res.on('data', (chunk: Buffer) => {
        chunks.push(chunk);
      });

      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8');
        const ok = res.statusCode && res.statusCode >= 200 && res.statusCode < 300;

        let parsed: any = raw;
        try {
          parsed = raw ? JSON.parse(raw) : {};
        } catch {
          // Se não conseguir parsear, mantém como string
        }

        if (!ok) {
          return reject(
            new Error(
              `HTTP ${res.statusCode} ${res.statusMessage}\nPATH: ${path}\nRESPONSE: ${raw}`
            )
          );
        }

        resolve({ statusCode: res.statusCode || 200, data: parsed });
      });
    });

    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Timeout na requisição para Evolution API'));
    });

    if (data) {
      req.write(data);
    }
    req.end();
  });
};

