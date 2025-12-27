-- Migration: Adicionar coluna user_timezone na tabela dispatches
-- Esta coluna armazenará o timezone do usuário para uso no agendamento
-- Evita a necessidade de buscar o timezone do backend principal durante o processamento

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dispatches' AND column_name = 'user_timezone') THEN
        ALTER TABLE dispatches ADD COLUMN user_timezone VARCHAR(100) DEFAULT 'America/Sao_Paulo';
        COMMENT ON COLUMN dispatches.user_timezone IS 'Fuso horário do usuário usado para agendamentos';
    END IF;
END
$$;

