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
  video_processor.py # download, transformacao FFmpeg e upload temporario
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
      2. opcionalmente transformar o video em uma copia tecnica nova
      3. criar container
      4. polling com backoff exponencial
      5. media_publish
      6. atualizar banco
      7. gravar logs
```

O contexto de publicacao usa `campaigns.caption` como unica legenda enviada para
a Meta. `campaigns.name` e `campaigns.description` sao campos internos do
painel e nunca devem ser usados como fallback de caption.

A criacao do container usa `POST /{ig-user-id}/media` e envia `caption`,
`media_type`, URL da midia, `cover_url` opcional e token como query parameters,
seguindo o contrato oficial de Content Publishing. O `httpx` aplica
percent-encoding UTF-8 reversivel, inclusive para emojis e quebras de linha.
Diagnosticos registram somente o transporte, comprimentos e SHA-256, sem expor
o texto da legenda.

## Processamento de Video

Quando `ENABLE_VIDEO_PROCESSING=true`, antes de criar o container na Meta o
publisher processa videos com FFmpeg em um pipeline isolado:

```text
Signed URL da biblioteca
  -> download para TEMP_DIRECTORY
  -> remocao completa de metadados
  -> processing_uuid e random_seed por publicacao
  -> metadados aleatorios novos
  -> reconstrucao completa do MP4
  -> filtros visuais imperceptiveis
  -> crop invisivel, scale minimo e FPS microvariavel
  -> randomizacao segura de encoder e audio
  -> validacao automatica com ffprobe
  -> upload temporario no mesmo bucket em _publisher_tmp/
  -> Signed URL temporaria para a Meta
  -> create_container / polling / media_publish
  -> remocao do objeto temporario e dos arquivos locais
```

O arquivo original da biblioteca nunca e alterado. Se download, FFmpeg, upload
temporario ou remocao falharem, o publisher registra logs detalhados e faz
fallback para a Signed URL original para nao interromper a publicacao.

O modo `VIDEO_PROCESSING_MODE=light_randomization` preserva qualidade visual
praticamente identica, mas gera um arquivo tecnicamente diferente a cada
publicacao. O FFmpeg reescreve video e audio, aplica `-map_metadata -1`,
reorganiza o container com `+faststart+use_metadata_tags`, recria handlers e
altera parametros seguros como CRF, preset, GOP, keyframes, scenecut, refs,
bframes, trellis, subme, aq-mode, aq-strength, psy-rd, me, merange, deblock,
lookahead, bitrate alvo, maxrate, bufsize, threads, slices, CABAC, open-GOP,
ipratio, pbratio, qpmin, qpmax, bitrate de audio, marcas MP4 e timestamps.
Use `PROCESSING_PRESET` com `fast`, `balanced` ou `quality` para ajustar custo
de CPU versus qualidade.

A Generation 4 adiciona variacoes tecnicas quase imperceptiveis:

- 2 a 5 filtros visuais leves entre brilho, contraste, saturacao, gama,
  nitidez, denoise minimo, temperatura e sombras.
- Crop invisivel de 2 a 4 pixels, com escala de volta para a resolucao
  original.
- Scale minimo temporario, sempre retornando para a resolucao original.
- FPS microvariavel proximo ao FPS original, preservando a duracao.
- Audio com volume ±0.5%, bitrate/sample rate variaveis e delay minimo quando
  existir stream de audio.
- Perfil ficticio de dispositivo apenas em metadados.

Cada processamento possui `processing_uuid=uuid4()` e `random_seed`. Esses
valores entram nos logs, nos nomes temporarios, no upload temporario e nos
metadados do video. O `random_seed` alimenta todas as escolhas variaveis do
FFmpeg para reduzir a chance de repetir a mesma combinacao.

Depois do FFmpeg, o publisher executa `ffprobe` no arquivo final e valida:
arquivo existente, duracao, resolucao, container, stream de video, codec de
video e stream de audio quando o original possuia audio. Se a validacao falhar,
o arquivo processado e removido e a publicacao segue com a Signed URL original.

`FFMPEG_TIMEOUT_SECONDS` impede processos presos. Quando o timeout expira, o
processo e encerrado, os temporarios sao removidos, `video_processing.timeout`
e registrado e o fallback para a Signed URL original e usado.

Na inicializacao, o publisher remove arquivos locais antigos em
`TEMP_DIRECTORY/video-processing` respeitando `TEMP_FILE_MAX_AGE_MINUTES`, sem
apagar arquivos recentes que possam pertencer a um processamento ativo.

`VIDEO_PROCESSING_DRY_RUN=true` executa download, FFmpeg, ffprobe e logs, mas
nao faz upload para Storage, nao chama a Meta e marca o job como concluido para
benchmark controlado.

Tambem sao registrados tamanho, duracao, container, codecs, FPS, bitrate,
resolucao, SHA-256, MD5 e CRC32 do arquivo original baixado e do arquivo final
processado. Quando `ENABLE_FRAME_HASH=true`, o processador extrai frame inicial,
central e final para gerar hashes perceptuais de diagnostico; esses hashes nao
bloqueiam publicacao.

Eventos principais:

```text
video_processing_started
video_processing_finished
video_processing_duration
video_processing_seed
video_processing_uuid
video_processing_profile
video_processing_filters
video_processing_hash_before
video_processing_hash_after
video_processing_size_before
video_processing_size_after
video_processing_audio_changed
video_processing_encoder
video_processing_ffmpeg_command
metadata_removed
metadata_generated
container_rebuilt
encoder_randomized
audio_reencoded
processing_failed
temporary_file_created
temporary_file_removed
```

Metricas principais:

```text
video_processing.started
video_processing.success
video_processing.failed
video_processing.timeout
video_processing.fallback
video_processing.ffprobe_failed
video_processing.cleanup_failed
video_processing.total_ms
video_processing.download_ms
video_processing.ffmpeg_ms
video_processing.upload_ms
video_processing.cleanup_ms
```

Os logs nunca registram Signed URLs, access tokens, client secrets ou headers de
autorizacao. Comandos FFmpeg sao sanitizados para mascarar caminhos locais.

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
ENABLE_VIDEO_PROCESSING
VIDEO_PROCESSING_MODE
ENABLE_VIDEO_VARIATION
ENABLE_COLOR_VARIATION
ENABLE_AUDIO_VARIATION
ENABLE_CROP
ENABLE_SCALE_VARIATION
ENABLE_FPS_VARIATION
ENABLE_MP4_REBUILD
ENABLE_DEVICE_PROFILE
ENABLE_METADATA_RANDOMIZATION
ENABLE_HASH_LOGGING
ENABLE_FRAME_HASH
VIDEO_VARIATION_LEVEL
VIDEO_RANDOM_SEED
VIDEO_RANDOMIZE_METADATA
VIDEO_REBUILD_CONTAINER
VIDEO_RANDOMIZE_ENCODER
VIDEO_RANDOMIZE_AUDIO
VIDEO_RANDOMIZE_TIMESTAMPS
VIDEO_RANDOMIZE_UUID
VIDEO_RANDOMIZE_BRANDS
FFMPEG_PATH
FFPROBE_PATH
FFMPEG_TIMEOUT_SECONDS
TEMP_DIRECTORY
TEMP_FILE_MAX_AGE_MINUTES
PROCESSING_PRESET
VIDEO_PROCESSING_DRY_RUN
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
