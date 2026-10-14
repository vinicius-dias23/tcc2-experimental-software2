-- Banco de DESTINO: pertence ao statement-service (consumidor, serviço de extrato).
-- Materializa contas e transações a partir das mensagens recebidas, qualquer que seja o
-- modelo de propagação, e registra cada mensagem recebida em received_events.

SET TIME ZONE 'UTC';

CREATE TABLE accounts (
    account_id          text PRIMARY KEY,
    account_type        text,
    balance             numeric(16, 2) NOT NULL,
    version             bigint NOT NULL DEFAULT 1,
    last_transaction_id text,
    last_correlation_id uuid,
    updated_at          timestamptz,
    applied_at          timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX accounts_applied_at_idx ON accounts (applied_at);

CREATE TABLE transactions (
    transaction_id   text PRIMARY KEY,
    step             integer NOT NULL,
    type             text NOT NULL,
    amount           numeric(16, 2) NOT NULL,
    name_orig        text NOT NULL,
    name_dest        text NOT NULL,
    old_balance_orig numeric(16, 2),
    new_balance_orig numeric(16, 2),
    old_balance_dest numeric(16, 2),
    new_balance_dest numeric(16, 2),
    is_fraud         boolean NOT NULL DEFAULT false,
    is_flagged_fraud boolean NOT NULL DEFAULT false,
    correlation_id   uuid,
    created_at       timestamptz,
    applied_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX transactions_created_at_idx ON transactions (created_at);

-- Uma linha por entidade recebida, inclusive as duplicadas e as fora de ordem.
-- É daqui que saem perda, duplicação e violação de ordem.
--   source     : 'domain-events' ou 'cdc'
--   entity     : 'transaction' ou 'account'
--   entity_key : transaction_id ou account_id
--   version    : versão da conta (vazio para a transação)
--   applied    : se a entidade alterou a projeção (false = descartada pela guarda de versão
--                ou transação já existente)
-- No Domain Events uma mensagem carrega a transação e as contas, então gera até três linhas
-- com o mesmo (topic, kafka_partition, kafka_offset); no CDC cada mensagem gera uma linha.
CREATE TABLE received_events (
    id              bigserial PRIMARY KEY,
    received_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    source          text NOT NULL,
    topic           text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    kafka_timestamp timestamptz,
    event_type      text NOT NULL,
    entity          text NOT NULL,
    entity_key      text NOT NULL,
    version         bigint,
    correlation_id  uuid,
    applied         boolean NOT NULL
);

CREATE INDEX received_events_correlation_idx ON received_events (correlation_id);
CREATE INDEX received_events_received_at_idx ON received_events (received_at);
