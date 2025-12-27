# Disparo-Clerky

Microserviço de Disparos para Clerky - Sistema de envio em massa de mensagens via WhatsApp.

## Características

- ✅ Templates de mensagens (texto, imagem, vídeo, áudio, arquivo, sequência)
- ✅ Disparos em massa com agendamento
- ✅ Validação de contatos
- ✅ Personalização de mensagens com variáveis
- ✅ Controle de velocidade de envio
- ✅ Exclusão automática de mensagens
- ✅ Integração com WebSocket para atualizações em tempo real

## Tecnologias

- Node.js + TypeScript
- Express.js
- PostgreSQL
- BullMQ (fila de jobs)
- Redis
- Socket.io (integração com backend principal)

## Instalação

```bash
npm install
```

## Configuração

1. Copie `.env.example` para `.env`
2. Configure as variáveis de ambiente:
   - `POSTGRES_URI`: URI do PostgreSQL (mesmo banco do backend principal)
   - `JWT_SECRET`: Mesmo secret JWT do backend principal
   - `SOCKET_URL`: URL do Socket.io do backend principal
   - `EVOLUTION_API_URL`: URL da Evolution API
   - `REDIS_HOST` e `REDIS_PORT`: Configuração do Redis

## Executar

```bash
# Desenvolvimento
npm run dev

# Produção
npm run build
npm start
```

## Estrutura

```
src/
├── config/          # Configurações (banco, constants)
├── controllers/      # Controllers da API
├── services/        # Lógica de negócio
├── routes/          # Rotas da API
├── queue/           # Processamento de jobs (BullMQ)
├── middleware/      # Middlewares (auth, error handler)
├── utils/           # Utilitários
└── server.ts        # Servidor principal
```

## API Endpoints

- `POST /api/dispatches/templates` - Criar template
- `GET /api/dispatches/templates` - Listar templates
- `GET /api/dispatches/templates/:id` - Buscar template
- `PUT /api/dispatches/templates/:id` - Atualizar template
- `DELETE /api/dispatches/templates/:id` - Deletar template
- `POST /api/dispatches` - Criar disparo
- `GET /api/dispatches` - Listar disparos
- `GET /api/dispatches/:id` - Buscar disparo
- `PUT /api/dispatches/:id` - Atualizar disparo
- `DELETE /api/dispatches/:id` - Deletar disparo
- `POST /api/dispatches/:id/start` - Iniciar disparo
- `POST /api/dispatches/:id/pause` - Pausar disparo
- `POST /api/dispatches/:id/resume` - Retomar disparo
- `POST /api/dispatches/upload-csv` - Upload CSV de contatos
- `POST /api/dispatches/process-input` - Processar texto de contatos
- `POST /api/dispatches/validate-contacts` - Validar contatos

## Integração com Backend Principal

O microserviço se integra com o backend principal via:
- **JWT**: Valida o mesmo token JWT
- **PostgreSQL**: Usa o mesmo banco de dados
- **Socket.io**: Conecta ao servidor Socket.io do backend principal para emitir atualizações

