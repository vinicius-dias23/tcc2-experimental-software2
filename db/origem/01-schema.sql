-- Banco de ORIGEM: pertence ao transaction-service (serviço de negócio).
-- O esquema é idêntico nos dois protótipos (Software A e Software B).
-- Nenhuma tabela de outbox existe aqui: o Software A publica direto na fila
-- e o Software B depende apenas do WAL (wal_level=logical, ver docker-compose.yml).

SET TIME ZONE 'UTC';

-- Contas do PaySim (clientes "C..." e comerciantes "M..."). Cada transação altera o saldo
-- de duas contas, por isso uma operação de negócio gera três alterações de linha.
--
-- version e last_correlation_id formam o oráculo do experimento: cada operação incrementa
-- version das contas que tocou e grava o identificador de correlação recebido na requisição,
-- que chega ao destino tanto pelo evento de domínio quanto pela linha capturada do WAL.
CREATE TABLE accounts (
    account_id          text PRIMARY KEY,
    account_type        text NOT NULL,
    balance             numeric(16, 2) NOT NULL,
    version             bigint NOT NULL DEFAULT 1,
    last_transaction_id text,
    last_correlation_id uuid,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX accounts_updated_at_idx ON accounts (updated_at);

-- Uma linha por transação do PaySim. Os saldos antes e depois são os calculados pelo
-- serviço (o CSV original tem saldos que não fecham).
CREATE TABLE transactions (
    transaction_id   text PRIMARY KEY,
    step             integer NOT NULL,
    type             text NOT NULL,
    amount           numeric(16, 2) NOT NULL,
    name_orig        text NOT NULL REFERENCES accounts (account_id),
    name_dest        text NOT NULL REFERENCES accounts (account_id),
    old_balance_orig numeric(16, 2),
    new_balance_orig numeric(16, 2),
    old_balance_dest numeric(16, 2),
    new_balance_dest numeric(16, 2),
    is_fraud         boolean NOT NULL DEFAULT false,
    is_flagged_fraud boolean NOT NULL DEFAULT false,
    correlation_id   uuid,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX transactions_created_at_idx ON transactions (created_at);

-- Registro da carga inicial (parcela do dataset pré-carregada).
CREATE TABLE seed_info (
    id                   integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    seeded_at            timestamptz NOT NULL DEFAULT now(),
    seed_fraction        numeric NOT NULL,
    data_file            text NOT NULL,
    transactions_total   integer NOT NULL,
    transactions_seeded  integer NOT NULL,
    accounts_seeded      integer NOT NULL,
    synthetic            boolean NOT NULL
);
