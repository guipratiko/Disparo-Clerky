import { Router } from 'express';
import { protect } from '../middleware/auth';
import {
  createTemplate,
  getTemplates,
  getTemplate,
  updateTemplate,
  deleteTemplate,
  uploadTemplateFile,
  uploadTemplateFileHandler,
} from '../controllers/templateController';
import {
  createDispatch,
  getDispatches,
  getDispatch,
  updateDispatch,
  startDispatch,
  pauseDispatch,
  resumeDispatch,
  deleteDispatch,
  uploadCSV,
  processCSVUpload,
  processInput,
  validateContactsNumbers,
} from '../controllers/dispatchController';

const router = Router();

// Todas as rotas requerem autenticação
router.use(protect);

// Rotas de Templates
router.post('/templates', createTemplate);
router.get('/templates', getTemplates);
router.get('/templates/:id', getTemplate);
router.put('/templates/:id', updateTemplate);
router.delete('/templates/:id', deleteTemplate);
router.post('/templates/upload', uploadTemplateFile, uploadTemplateFileHandler);

// Rotas de Validação
router.post('/validate-contacts', validateContactsNumbers);

// Rotas de Upload/Processamento
router.post('/upload-csv', uploadCSV, processCSVUpload);
router.post('/process-input', processInput);

// Rotas de Disparos
router.post('/', createDispatch);
router.get('/', getDispatches);
// Rotas específicas devem vir antes das rotas genéricas com :id
router.post('/:id/start', startDispatch);
router.post('/:id/pause', pauseDispatch);
router.post('/:id/resume', resumeDispatch);
router.get('/:id', getDispatch);
router.put('/:id', updateDispatch);
router.delete('/:id', deleteDispatch);

export default router;

