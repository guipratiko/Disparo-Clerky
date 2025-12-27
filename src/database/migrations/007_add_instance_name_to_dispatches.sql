-- Migration: Adicionar coluna instance_name na tabela dispatches
-- Isso permite salvar o instanceName quando criamos o disparo, evitando buscar no scheduler

-- Adicionar coluna instance_name
ALTER TABLE dispatches 
ADD COLUMN IF NOT EXISTS instance_name VARCHAR(255);

-- Criar índice para melhorar performance
CREATE INDEX IF NOT EXISTS idx_dispatches_instance_name ON dispatches(instance_name);

-- Comentário
COMMENT ON COLUMN dispatches.instance_name IS 'Nome da instância (instanceName) usado na Evolution API. Salvo ao criar o disparo para evitar buscar no scheduler.';

