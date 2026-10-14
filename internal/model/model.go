// Package model define as entidades do domínio (dataset PaySim) e o envelope dos eventos
// de domínio publicados pelo Software A.
package model

import "time"

// Account espelha a tabela accounts. As tags JSON coincidem com os nomes das colunas, de
// modo que a mesma struct decodifica tanto a conta dentro do evento de domínio quanto a linha
// capturada pelo Debezium (após o ExtractNewRecordState). Valores monetários trafegam como
// string (decimal.handling.mode=string no Debezium) para não perder precisão.
type Account struct {
	AccountID         string     `json:"account_id"`
	AccountType       string     `json:"account_type"`
	Balance           string     `json:"balance"`
	Version           int64      `json:"version"`
	LastTransactionID *string    `json:"last_transaction_id"`
	LastCorrelationID *string    `json:"last_correlation_id"`
	UpdatedAt         *time.Time `json:"updated_at"`
}

// Transaction espelha a tabela transactions: uma linha por operação do PaySim. Os saldos
// antes e depois são os calculados pelo serviço, não os do CSV.
type Transaction struct {
	TransactionID  string     `json:"transaction_id"`
	Step           int        `json:"step"`
	Type           string     `json:"type"`
	Amount         string     `json:"amount"`
	NameOrig       string     `json:"name_orig"`
	NameDest       string     `json:"name_dest"`
	OldBalanceOrig *string    `json:"old_balance_orig"`
	NewBalanceOrig *string    `json:"new_balance_orig"`
	OldBalanceDest *string    `json:"old_balance_dest"`
	NewBalanceDest *string    `json:"new_balance_dest"`
	IsFraud        bool       `json:"is_fraud"`
	IsFlaggedFraud bool       `json:"is_flagged_fraud"`
	CorrelationID  *string    `json:"correlation_id"`
	CreatedAt      *time.Time `json:"created_at"`
}

// Tipos de transação do PaySim.
var TransactionTypes = map[string]bool{"CASH_IN": true, "CASH_OUT": true, "DEBIT": true, "PAYMENT": true, "TRANSFER": true}

// CreditsOrigin diz se o tipo aumenta o saldo da conta de origem (CASH_IN: o cliente deposita
// dinheiro por um comerciante) ou o diminui (os demais tipos).
func CreditsOrigin(txType string) bool { return txType == "CASH_IN" }

// EventTransactionCompleted é o único tipo de evento de domínio do Software A.
const EventTransactionCompleted = "TransactionCompleted"

// DomainEvent é o envelope publicado pelo transaction-service no Software A: um evento por
// operação de negócio, com a transação e as contas (origem e destino) após a mudança.
// No Software B a mesma operação gera uma mensagem por linha alterada (a transação e cada
// conta), o que dá ao PaySim a razão alta entre alterações de linha e operações.
type DomainEvent struct {
	EventID       string      `json:"event_id"`
	EventType     string      `json:"event_type"`
	OccurredAt    time.Time   `json:"occurred_at"`
	CorrelationID string      `json:"correlation_id"`
	AggregateID   string      `json:"aggregate_id"`
	Transaction   Transaction `json:"transaction"`
	Accounts      []Account   `json:"accounts"`
}
