-- Migration: Criar tabelas de Disparos (templates, dispatches)
-- Este arquivo cria toda a estrutura base do sistema de Disparos

-- Habilitar extensão UUID (se ainda não estiver habilitada)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- TABELA: templates
-- ============================================
CREATE TABLE IF NOT EXISTS templates (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id VARCHAR(24) NOT NULL,
  name VARCHAR(255) NOT NULL,
  type VARCHAR(50) NOT NULL CHECK (type IN ('text', 'image', 'image_caption', 'video', 'video_caption', 'audio', 'file', 'sequence')),
  content JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_templates_user_id ON templates(user_id);
CREATE INDEX IF NOT EXISTS idx_templates_type ON templates(type);
CREATE INDEX IF NOT EXISTS idx_templates_user_type ON templates(user_id, type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_templates_user_name ON templates(user_id, name);

-- ============================================
-- TABELA: dispatches
-- ============================================
CREATE TABLE IF NOT EXISTS dispatches (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id VARCHAR(24) NOT NULL,
  instance_id VARCHAR(24) NOT NULL,
  template_id UUID REFERENCES templates(id) ON DELETE SET NULL,
  name VARCHAR(255) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'paused', 'completed', 'failed')),
  settings JSONB NOT NULL DEFAULT '{}',
  schedule JSONB,
  contacts_data JSONB NOT NULL DEFAULT '[]',
  stats JSONB NOT NULL DEFAULT '{"sent": 0, "failed": 0, "invalid": 0, "total": 0}',
  default_name VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dispatches_user_id ON dispatches(user_id);
CREATE INDEX IF NOT EXISTS idx_dispatches_status ON dispatches(status);
CREATE INDEX IF NOT EXISTS idx_dispatches_instance_id ON dispatches(instance_id);
CREATE INDEX IF NOT EXISTS idx_dispatches_user_status ON dispatches(user_id, status);
CREATE INDEX IF NOT EXISTS idx_dispatches_template_id ON dispatches(template_id);

-- ============================================
-- TRIGGERS
-- ============================================
CREATE OR REPLACE FUNCTION update_templates_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_templates_updated_at
BEFORE UPDATE ON templates
FOR EACH ROW
EXECUTE FUNCTION update_templates_updated_at();

CREATE OR REPLACE FUNCTION update_dispatches_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_dispatches_updated_at
BEFORE UPDATE ON dispatches
FOR EACH ROW
EXECUTE FUNCTION update_dispatches_updated_at();

