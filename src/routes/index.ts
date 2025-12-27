import { Router } from 'express';
import dispatchRoutes from './dispatch.routes';

const router = Router();

// Rotas de Disparos
router.use('/dispatches', dispatchRoutes);

export default router;

