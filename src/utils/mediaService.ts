/**
 * Utilitário para upload de arquivos para Media Service
 */

import FormData from 'form-data';
import axios from 'axios';
import { MEDIA_SERVICE_CONFIG } from '../config/constants';

const getContentType = (messageType: string): string => {
  const contentTypeMap: Record<string, string> = {
    imageMessage: 'image/jpeg',
    audioMessage: 'audio/ogg',
    videoMessage: 'video/mp4',
    documentMessage: 'application/pdf',
    stickerMessage: 'image/webp',
  };

  return contentTypeMap[messageType] || 'application/octet-stream';
};

export const uploadFileToService = async (
  fileBuffer: Buffer,
  fileName: string,
  contentType: string
): Promise<{ url: string; fullUrl: string; fileName: string } | null> => {
  try {
    const formData = new FormData();
    formData.append('file', fileBuffer, {
      filename: fileName,
      contentType,
    });

    const response = await axios.post(
      `${MEDIA_SERVICE_CONFIG.URL}/upload`,
      formData,
      {
        headers: {
          ...formData.getHeaders(),
          Authorization: `Bearer ${MEDIA_SERVICE_CONFIG.TOKEN}`,
        },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      }
    );

    if (response.data.success) {
      return {
        url: response.data.url,
        fullUrl: response.data.fullUrl,
        fileName: fileName,
      };
    }

    return null;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
    console.error('❌ Erro ao fazer upload de arquivo para MidiaService:', errorMessage);
    return null;
  }
};

