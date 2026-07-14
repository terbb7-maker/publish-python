# Terbb Python Publisher

Publisher simples e continuo para publicar `campaign_jobs` no Instagram pela
Instagram Graph API. O painel Next.js continua sendo apenas CRUD: login,
conexao de contas, upload de midias, criacao de campanhas e gerenciamento.

O Python e o unico responsavel por publicar.

## Principio

```text
Loop infinito
  -> preencher slots livres de concorrencia
  -> buscar ate 100 jobs elegiveis por claim
  -> reservar com FOR UPDATE SKIP LOCKED
  -> manter tasks asyncio continuamente ocupadas
  -> publicar contas diferentes em paralelo
  -> salvar resultado
  -> voltar ao inicio
```

Nao existe lifecycle, calculo de dashboard ou service encadeado no caminho
quente. O unico refresh agregado permitido e o refresh minimo de
`campaign_accounts.jobs_*` e `campaigns.status`, derivado de `campaign_jobs`,
para manter o painel Next.js compativel com o publisher.

## Componentes

```text
app/
  main.py        # boot e shutdown
  config.py      # env vars
  database.py    # pool Postgres e Supabase Storage
  repository.py  # SQL de claim, status, retry, heartbeat e logs
  instagram.py   # chamadas HTTP para Instagram Graph API
  publisher.py   # publica um job do inicio ao fim
  scheduler.py   # slots continuos e concorrencia
  logger.py      # logs JSON e redacao de dados sensiveis
  metrics.py     # contadores em memoria
sql/
  001_minimal_publisher_schema.sql
```

## Fluxo de Publicacao

```text
Scheduler
  -> calcula slots livres
  -> Repository.claim_due_jobs(limit=slots_livres)
  -> asyncio.create_task(job)
  -> asyncio.wait(..., FIRST_COMPLETED)
  -> lock local por social_account_id
  -> Publisher.publish(job_id)
      1. obter contexto
      2. criar container
      3. polling com backoff exponencial
      4. media_publish
      5. atualizar banco
      6. gravar logs
```

## Estados

O publisher usa apenas:

```text
scheduled
running
completed
failed
cancelled
```

Campos de idempotencia ficam em `campaign_jobs.metadata_safe`:

```text
provider_container_id
provider_media_id
provider_status
last_error
last_heartbeat_at
```

`attempt_count`, `last_error_code` e `last_error_message_safe` usam as colunas
existentes da tabela.

## SQL Obrigatorio

Antes de rodar em producao, aplique:

```bash
psql "$SUPABASE_DATABASE_URL" -f sql/001_minimal_publisher_schema.sql
```

Esse SQL adiciona o estado `completed` ao enum `campaign_job_status` e cria
indices alinhados ao claim simples do publisher.

No projeto principal, a compatibilidade completa do painel e versionada pelas
migrations:

```text
supabase/migrations/20260711120000_campaign_job_completed_status.sql
supabase/migrations/20260711120500_campaign_publisher_panel_sync.sql
```

## Concorrencia

- `FOR UPDATE SKIP LOCKED` impede dois processos de reservarem o mesmo job.
- O claim nao reserva uma conta que ja possui job `running` com lease valido.
- O numero de tasks em execucao limita a concorrencia global.
- Um lock local por `social_account_id` impede duas publicacoes simultaneas na
  mesma conta dentro do mesmo processo.
- Contas diferentes publicam em paralelo.
- O Scheduler nao espera lotes inteiros: assim que uma task termina, o slot fica
  disponivel para novo claim.

## Idempotencia

O publisher nunca republica se o resultado da etapa final for desconhecido.

```text
sem container -> criar container
container salvo -> reutilizar container
media salvo -> marcar completed
status publishing sem media_id -> marcar failed para evitar duplicidade
```

Se o processo cair antes de `media_publish`, o job pode continuar usando o
container salvo. Se cair durante `media_publish`, o job e bloqueado como
`failed` com `publish_result_unknown`, evitando publicar duas vezes.

## Retries

Falhas temporarias voltam para `scheduled` com backoff exponencial e jitter.

Falhas permanentes viram `failed`:

- token expirado
- permissao insuficiente
- midia invalida
- validacao da Meta

Rate limit vira retry.

## Heartbeat e Lease

Durante uma publicacao, o publisher atualiza `reserved_at` e
`metadata_safe.last_heartbeat_at` periodicamente. Se uma tentativa de heartbeat
falhar por erro transitorio, ele continua tentando com backoff exponencial e
jitter.

Jobs `running` sem heartbeat recente voltam para `scheduled`, exceto quando o
status salvo e `publishing`, pois esse ponto pode ter chamado `media_publish`.
Nesse caso o job vira `failed` para evitar republicacao.

## Variaveis

Copie `.env.example` para `.env`.

Obrigatorias:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
SUPABASE_DATABASE_URL
META_TOKEN_ENCRYPTION_KEY
INSTAGRAM_API_VERSION
```

## Rodar Local

```bash
cd python-publisher
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
psql "$SUPABASE_DATABASE_URL" -f sql/001_minimal_publisher_schema.sql
python -m app.main
```

## Docker

```bash
cd python-publisher
cp .env.example .env
docker compose up --build
```

## Railway

1. Criar um projeto no Railway.
2. Conectar o repositorio GitHub.
3. Definir root directory como `python-publisher`.
4. Usar Dockerfile.
5. Configurar como background worker.
6. Cadastrar as variaveis do `.env.example`.
7. Aplicar `sql/001_minimal_publisher_schema.sql` no Supabase antes do primeiro
   start.
8. Configurar restart policy para sempre reiniciar em falha.
9. Monitorar logs JSON no painel.

## Render

1. Criar um `Background Worker`.
2. Conectar o repositorio GitHub.
3. Definir root directory como `python-publisher`.
4. Usar Docker runtime.
5. Cadastrar as variaveis do `.env.example`.
6. Aplicar `sql/001_minimal_publisher_schema.sql` no Supabase.
7. Usar o comando padrao do container:

```bash
python -m app.main
```

## Observabilidade

Logs JSON em stdout:

```text
scheduler_started
claim_finished
job_completed
job_instagram_error
job_timeout
job_unexpected_error
container_poll
```

Eventos persistidos em `campaign_events` sao best-effort e nunca derrubam uma
publicacao.

O polling do container registra cada tentativa em stdout, mas so grava no banco
quando ha mudanca relevante de estado: container criado, container pronto,
publishing, completed ou failed.
