# TCC: protótipo experimental 2 (PaySim)

Protótipos do experimento que compara **Domain Events** e **Change Data Capture** como modelos de
propagação de mudanças entre microsserviços sob falhas de disponibilidade. Este repositório usa o
**Dataset 2**, o [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) (transações financeiras
simuladas), e traz os scripts de carga e de injeção de falha do **Cenário 1 (fila indisponível)**.
A estrutura, os scripts e o painel seguem o protótipo 1
([tcc2-experimental-software1](https://github.com/vinicius-dias23/tcc2-experimental-software1), Olist),
para que os dois conjuntos de dados sejam medidos da mesma forma.

| | Software A: Domain Events | Software B: Change Data Capture |
|---|---|---|
| Quem propaga | o próprio `transaction-service`, logo após o commit | Debezium (Kafka Connect) lendo o WAL do PostgreSQL |
| Outbox | **não** (janela de dual write exposta de propósito) | não se aplica |
| Mensagens por operação | 1 evento por transação | 1 mensagem por linha alterada: a transação e as duas contas (3) |
| Subir | `docker compose up backend-domain-events` | `docker compose up backend-change-data-capture` |

Todo o resto é idêntico nos dois: o mesmo binário Go (o modo vem de `PROPAGATION_MODE`), o mesmo
esquema, os mesmos endpoints, o mesmo consumidor, os mesmos bancos e o mesmo cluster Kafka.

**Por que o PaySim.** Cada transação altera três linhas (a transação e os saldos das contas de origem
e de destino), contra uma operação de negócio só. É a "alta razão entre alterações de linha e operações
de negócio" da Seção 4.3: no CDC cada transação vira três mensagens, no Domain Events uma. As contas de
destino se repetem muito no dataset, o que coloca várias transações concorrentes sobre a mesma conta e
torna a ordem por conta mensurável.

## Arquitetura

```
                     Software A (domain-events)
                  ┌──────── publica após o commit ─────────┐
                  │                                        ▼
cliente ──HTTP──▶ transaction-service ──▶ postgres-origem   Kafka (3 brokers, KRaft) ──▶ statement-service ──▶ postgres-destino
 (scripts/carga)  (Go, :8080)             (wal_level=logical)  ▲                        (Go, :8081)
                                                 │             │
                                                 └── WAL ──▶ Debezium/Kafka Connect (:8083)
                                                      Software B (cdc)
```

- **transaction-service** (origem): `POST /transactions`, `GET /transactions/{id}`,
  `GET /accounts/{id}`, `GET /healthz`, `GET /metrics`. Cada transação, numa única transação do banco,
  atualiza o saldo e a versão das duas contas (criando a conta se for nova) e grava a linha em
  `transactions`. O cabeçalho `X-Correlation-ID` é gravado nas três linhas e propagado até o destino.
- **statement-service** (destino, serviço de extrato): consome a fila, materializa contas e transações
  e registra **cada entidade recebida** em `received_events`. É dali que saem perda, duplicação e ordem.
- **Kafka**: 3 brokers `apache/kafka:3.8.0` em modo KRaft, tópicos com 6 partições, fator de
  replicação 3 e `min.insync.replicas=2`. Tópicos: `paysim.domain-events.transactions` (A, chave = conta
  de origem) e `paysim.cdc.public.transactions` / `paysim.cdc.public.accounts` (B, chave = chave primária).
- **PostgreSQL 16**: um banco por serviço. `wal_level=logical` nos dois modos, para que a origem seja
  idêntica; o slot de replicação só existe no Software B.

### Regra de saldo

`CASH_IN` credita a conta de origem e debita a de destino; `CASH_OUT`, `PAYMENT`, `TRANSFER` e `DEBIT`
debitam a origem e creditam o destino. Uma conta nasce com o saldo antigo informado na primeira linha em
que aparece (`oldbalanceOrg` ou `oldbalanceDest`) e daí em diante o saldo é o que o serviço calcula. Não
há verificação de saldo: o PaySim tem saldos que não fecham, e recusar operações tiraria linhas do
dataset da carga. A versão da conta sobe 1 a cada transação que a toca.

## Pré-requisitos

- Docker com Compose v2.20 ou mais novo (`docker compose version`). Reserve uns 6 GB de RAM.
- Python 3.10+ para os scripts:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r scripts/requirements.txt
```

## Dados

O dataset exige login no Kaggle, por isso não está no repositório. O CSV
(`PS_20174392719_1491204439457_log.csv`, 6.362.620 linhas, ~470 MB) vai em `data/paysim/`. O script
abaixo põe o arquivo no lugar e confere o formato:

```bash
python -m scripts.dados.preparar_paysim --kaggle                       # CLI do Kaggle (~/.kaggle/kaggle.json)
python -m scripts.dados.preparar_paysim --zip ~/Downloads/archive.zip  # zip do botão Download
python -m scripts.dados.preparar_paysim --csv caminho/do/arquivo.csv   # CSV já extraído
```

**Carga inicial e carga de teste.** As linhas ficam na ordem do arquivo (por `step`) e recebem o
identificador `T` + posição (`T000000001` é a primeira). Das primeiras `PAYSIM_MAX_ROWS` linhas
(padrão **1.000.000**; `0` usa as 6,3 milhões), os primeiros 70% (`SEED_FRACTION`) são gravados direto
nas duas bases pelo serviço `seed` quando o ambiente sobe, para que os testes não comecem com bancos
vazios. O restante é reproduzido pela API durante os testes, uma transação por linha. Quando esse
conjunto acaba numa carga longa, ele é reaproveitado com `transaction_id` novos (as contas são as
mesmas, então as cópias movimentam contas que já existem).

O limite de 1 milhão de linhas existe porque cada execução de cenário recria as bases do zero
(`down -v`), e o seed das 6,3 milhões de linhas leva alguns minutos por execução. Com 1 milhão, o
seed grava 700 mil transações e cerca de 1,2 milhão de contas em cada banco, e sobram 300 mil
transações para a carga (50 min a 100 ops/s antes de começar a clonar). Para usar o dataset inteiro,
`PAYSIM_MAX_ROWS=0` no `.env`. O valor usado fica em `seed_info` e nos resultados.

**Amostra sintética (só para validar o pipeline).** Sem o dataset, gere um CSV no mesmo formato:

```bash
python -m scripts.dados.gerar_amostra_sintetica --linhas 200000 --saida data/paysim-sintetico
echo "PAYSIM_DATA_DIR=./data/paysim-sintetico" > .env
```

A pasta leva um marcador `_SINTETICO`; o seed registra isso em `seed_info.synthetic` e os scripts
avisam no terminal. Não use esses dados no TCC.

## Subindo os protótipos

```bash
docker compose up backend-domain-events          # Software A
docker compose up backend-change-data-capture    # Software B
```

Acrescente `-d` para liberar o terminal. O serviço `backend-*` só fica pronto depois que os bancos
estão saudáveis, a carga inicial terminou, os tópicos existem e (no B) o conector Debezium está
`RUNNING`.

Os dois usam as mesmas portas, então **derrube um antes de subir o outro**. O jeito mais simples de
alternar, que também zera as bases:

```bash
python -m scripts.comum.ambiente cdc             # ou domain-events
# equivalente a:
docker compose --profile domain-events --profile cdc down -v
docker compose up -d --wait backend-change-data-capture
```

Teste rápido da API:

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/transactions -H 'Content-Type: application/json' -d '{
  "transaction_id": "teste-1", "step": 1, "type": "TRANSFER", "amount": "1500.00",
  "name_orig": "C123", "name_dest": "C456", "old_balance_orig": "5000", "old_balance_dest": "0"}'
curl -s localhost:8080/accounts/C123
curl -s localhost:8081/metrics
```

Portas no host: API `8080`, consumidor `8081`, Kafka Connect `8083` e vigia do conector `8086` (só no
B), banco de origem `5432` e banco de destino `5433` (usuário, senha e base: `paysim`). São as mesmas
do protótipo 1, então os dois protótipos não sobem ao mesmo tempo na mesma máquina.

## Painel

Uma página no navegador com o diagrama dos serviços, o estado de cada um ao vivo, a vazão entre
eles e botões para injetar falhas e disparar os scripts. Roda na máquina, com o venv ativo, fora
do Docker Compose (assim sobrevive ao `down -v` que os scripts fazem entre execuções):

```bash
python -m scripts.painel            # abre em http://localhost:8090
```

- **Diagrama.** Mostra o software que estiver no ar (A ou B), com cada contêiner em verde (no ar),
  âmbar (iniciando, não saudável ou sem responder ao `/healthz`), roxo (congelado com `pause`),
  vermelho (parado) ou cinza (não criado). As setas mostram a vazão lida dos `/metrics`, as falhas de
  publicação no A e, no B, o estado da tarefa do Debezium e o atraso do slot de replicação. Clicar
  num serviço abre os detalhes e os botões **Parar** (`stop`), **Matar** (`kill`), **Congelar**
  (`pause`), **Religar** e **Reiniciar**.
- **Injeção rápida.** Atalhos para derrubar a fila inteira, tirar o quórum (2 brokers), derrubar
  1 broker, o consumidor, um dos bancos ou o Debezium, com o tipo de parada escolhido; e **Restaurar
  tudo**, que religa o que estiver parado ou congelado. São os mesmos comandos `docker compose` do
  Cenário 1.
- **Scripts.** Sobe ou troca o backend, gera carga (constante, rajada ou represamento) e executa o
  Cenário 1 com os parâmetros do formulário, mostrando a saída do script. Enquanto o cenário ou a troca
  de backend rodam, os botões de falha ficam desativados para não interferir na execução.
- **Gráfico e linha do tempo.** Vazão dos últimos 5 minutos, com faixas onde algum serviço estava
  fora do ar, e o registro de cada mudança de estado e de cada ação feita pelo painel.

O painel só escuta em `127.0.0.1`, porque controla o Docker. As falhas manuais servem para explorar
o comportamento; as medições do TCC continuam saindo dos scripts, que registram tudo em
`resultados/`.

## Testes de carga

Rodam contra o backend que estiver no ar (o modo é detectado sozinho):

```bash
# taxa constante: vazão, latência da API e atraso de propagação
python -m scripts.carga.teste_carga constante --taxa 200 --duracao 300

# rajada: N operações o mais rápido possível
python -m scripts.carga.teste_carga rajada --operacoes 50000 --concorrencia 300

# represamento: para o consumidor, enche a fila, religa e mede a drenagem do acúmulo
python -m scripts.carga.teste_carga represamento --operacoes 100000 --concorrencia 300
```

O `represamento` é o caso em que as filas têm muitas mensagens para sincronizar ao mesmo tempo: com
100 mil transações, ficam represadas 100 mil mensagens no A e 300 mil no B. A drenagem termina quando
toda transação commitada chegou ao destino e as contas e transações tocadas batem com a origem. A saída
vai para `resultados/carga/<execução>/` (`ledger.csv`, exportações das bases, `resumo.json` e
`resumo_carga.json` com vazão da API, tempo de drenagem e vazão de consumo). `--reset --modo cdc`
reinicializa o ambiente antes.

## Cenário 1: indisponibilidade da fila

```bash
# desenho completo (janelas de 5, 15, 30 e 60 min) para um software
python -m scripts.cenario1.fila_indisponivel --modo domain-events
python -m scripts.cenario1.fila_indisponivel --modo cdc

# os dois softwares, janelas escolhidas, 3 repetições
python -m scripts.cenario1.fila_indisponivel --modo ambos --janelas 5,15 --repeticoes 3

# ensaio rápido
python -m scripts.cenario1.fila_indisponivel --modo cdc --janela-segundos 60 --regime 30 --recuperacao 30
```

Cada execução:

1. reinicializa o ambiente (`down -v` e `up`), o que recarrega as bases, recria os tópicos e o slot;
2. **regime**: carga constante (`--taxa`, padrão 100 ops/s) por `--regime` segundos;
3. **falha**: `docker compose stop` nos três brokers pela duração da janela, com a carga continuando;
4. **recuperação**: sobe os brokers, mantém a carga por `--recuperacao` segundos, para a carga e
   acompanha até origem e destino convergirem ou até não chegar nada por `--sem-progresso` segundos.

Opções de injeção: `--parada kill` (SIGKILL), `--parada pause` (processo congelado e inalcançável) e
`--brokers kafka-2,kafka-3` (derruba só parte do cluster, deixando as partições sem quórum para
`acks=all`). No B, `--reiniciar-conector-falho` faz a intervenção manual: reinicia a tarefa do
Debezium que ficou em `FAILED` depois que o reinício automático desistiu, e registra a intervenção.
`--sem-recuperacao` desliga a recuperação automática dos dois softwares (ver abaixo).

### O que sai em `resultados/cenario1/<execução>/`

| Arquivo | Conteúdo |
|---|---|
| `ledger.csv` | uma linha por requisição: horário, `transaction_id`, tipo, contas, `correlation_id`, status HTTP, latência, versões das contas |
| `serie_temporal.csv` | amostra a cada `--intervalo` s: sucesso e p99 da API, escritas commitadas, mensagens entregues, lacuna de propagação, divergência entre as bases, falhas de publicação, eventos esperando retentativa, recuperados e declarados perdidos (A), estado do conector e reinícios automáticos (B), tamanho do banco, WAL e WAL retido pelo slot |
| `marcos.json` | instantes de início e fim de cada fase e os parâmetros |
| `origem_*.csv`, `destino_*.csv` | contas e transações tocadas na execução e as mensagens recebidas, para refazer a análise offline |
| `logs/` | logs dos serviços, brokers e Kafka Connect |
| `resumo.json` | as métricas da Seção 4.5 calculadas para a execução |

`resultados/cenario1/consolidado.csv` acumula uma linha por execução, com as mesmas colunas do
protótipo 1 (as de divergência viram `div_transacoes_ausentes` e `div_contas`). Para recalcular uma
execução: `python -m scripts.comum.analise resultados/cenario1/<execução>`.

### Como as métricas são calculadas

- **Operação commitada**: resposta 2xx da API, que só responde depois do commit. Requisições sem
  resposta contam se a transação existe na origem com o mesmo `correlation_id`.
- **Perda de eventos**: operação commitada da qual nenhuma mensagem chegou ao destino (por
  `correlation_id`). Também é reportada a perda de mensagens, porque no CDC cada transação gera três
  mensagens (duas quando origem e destino são a mesma conta).
- **Duplicação**: mensagens repetidas para a mesma `(correlation_id, entidade, chave)`.
- **Violação de ordem**: mensagem de conta com versão menor do que outra já recebida da mesma conta.
  No A, o evento leva a chave da conta de origem, então a conta de destino de transações diferentes
  pode chegar por partições diferentes; no B, cada conta vai sempre para a mesma partição.
- **Divergência final**: contas tocadas na execução cuja versão ou saldo diferem entre origem e
  destino, e transações da execução ausentes no destino.
- **Tempo até detecção**: primeira amostra após o início da falha com sinal observável. São
  reportados o sinal genérico (lacuna de propagação acima da linha de base), o erro de publicação no
  A e, no B, a tarefa do conector fora de `RUNNING` ou o atraso do slot acima de 16 MB.
- **Tempo até retomada**: da volta dos brokers até a primeira mensagem **emitida** (timestamp do
  Kafka) e até a primeira mensagem **entregue** ao destino.
- **Tempo até convergência**: da volta dos brokers até a primeira amostra com divergência zero.
- **Efeito colateral**: latência p99 e taxa de sucesso da API por fase; tamanho do banco, do WAL e
  do WAL retido pelo slot por fase.
- **Recuperação**: no A, eventos publicados por uma retentativa, eventos declarados perdidos e a
  **perda não declarada** (perda medida pelo oráculo menos os declarados perdidos), além dos
  confirmados com atraso pelo broker; no B, reinícios automáticos, se o vigia desistiu e quando.

## Recuperação automática

Igual ao protótipo 1. Os dois softwares tentam se recuperar sozinhos com **a mesma política**:
3 tentativas, a primeira 1 s depois da falha e cada uma o dobro da anterior (1 s, 2 s, 4 s). Esgotadas
as tentativas, a falha é declarada e contada.

| | Software A: retentativa da publicação | Software B: reinício da tarefa do Debezium |
|---|---|---|
| Quando age | a publicação síncrona falhou depois do commit | o conector ou a tarefa entrou em `FAILED` |
| O que faz | espera o resultado real da publicação; se o broker recusou, reenvia o evento a cada tentativa | pede `POST /connectors/<nome>/restart?includeTasks=true&onlyFailed=true` |
| Esgotado | o evento é **declarado perdido** (`events_declared_lost_total`) | o vigia **desiste** (`connector_restart_gave_up=1`): intervenção manual necessária |
| Onde fica | memória do `transaction-service` (até `PUBLISH_RETRY_BUFFER` eventos; acima disso, declarado perdido) | serviço `connect-watchdog`, papel que em produção é do operador do Connect (ex.: `autoRestart` do Strimzi) |
| Variáveis | `PUBLISH_RETRY_ATTEMPTS`, `PUBLISH_RETRY_BACKOFF`, `PUBLISH_RETRY_MULTIPLIER`, `PUBLISH_RETRY_BUFFER` | `CONNECT_RESTART_ATTEMPTS`, `CONNECT_RESTART_BACKOFF`, `CONNECT_RESTART_MULTIPLIER`, `CONNECT_RESTART_RESET_AFTER` |

A fila de retentativa do A é **só em memória**, de propósito: um registro durável do que falta
publicar seria um Outbox, que o Software A não usa. Os valores usados em cada execução ficam em
`marcos.json` (`parametros.recuperacao_automatica`). `--sem-recuperacao` nos scripts (ou
`PUBLISH_RETRY_ATTEMPTS=0` e `CONNECT_RESTART_ATTEMPTS=0`) volta ao comportamento sem retentativa.

### Disco limitado no banco de origem

No B, o slot de replicação segura o WAL enquanto a fila está fora. Para observar o ponto em que isso
esgota o disco e o banco passa a recusar escritas sem encher o disco da máquina, use a sobreposição
que coloca os dados da origem num tmpfs de tamanho fixo (precisa caber a carga inicial: com o padrão
de 1 milhão de linhas, a origem começa com uns 400 MB):

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.disco-limitado.yml
export ORIGEM_DISCO_LIMITE=1g
python -m scripts.cenario1.fila_indisponivel --modo cdc --janelas 15 --taxa 400
```

## Decisões de implementação que afetam as medições

As mesmas do protótipo 1 (publicação síncrona no A com `PUBLISH_TIMEOUT` de 5 s e resposta de
sucesso depois do commit, falha de publicação decidida pelo resultado real do produtor,
`producer.delivery.timeout.ms` no máximo no Connect, `snapshot.mode=no_data`, consumidor com entrega
pelo menos uma vez e guarda de versão). As específicas do PaySim:

- **Uma operação, três linhas.** A transação do banco atualiza as duas contas e insere a transação.
  As contas são travadas sempre em ordem alfabética, para que transferências cruzadas não entrem em
  impasse.
- **Chave do evento no A.** O evento de domínio carrega a transação e as duas contas e usa a conta de
  origem como chave de partição. A ordem da conta de destino entre eventos diferentes não é garantida
  pelo Kafka, só pela guarda de versão do consumidor; isso aparece como violação de ordem.
- **Saldos calculados.** Os saldos de `transactions` (antes e depois) são os calculados pelo serviço,
  não os do CSV, que não fecham em boa parte das linhas.
- **Subconjunto do dataset.** Por padrão, 1 milhão de linhas (`PAYSIM_MAX_ROWS`), pelo custo de
  recriar as bases em cada execução. É um parâmetro do experimento e entra nas ameaças à validade se
  for mantido.
- **Decomposição.** São dois serviços (origem e destino), o suficiente para o Cenário 1. Os demais
  cenários ainda não foram portados para este protótipo.

## Estrutura

```
cmd/transaction-service  serviço de negócio (origem)
cmd/statement-service    consumidor (destino, extrato)
cmd/seed                 carga inicial a partir do CSV
cmd/connect-watchdog     reinício automático da tarefa do Debezium (Software B)
internal/                modelo, publicação e retentativa no Kafka, política de retentativa, métricas, configuração
db/origem, db/destino    esquemas SQL
infra/kafka              criação dos tópicos
infra/debezium           configuração e registro do conector
scripts/carga            gerador de carga e testes de carga
scripts/cenario1         injeção de falha do Cenário 1
scripts/comum            dataset, controle do ambiente, coleta e análise
scripts/painel           painel web com o diagrama, as falhas e os scripts
scripts/dados            preparação do CSV real e amostra sintética
```
