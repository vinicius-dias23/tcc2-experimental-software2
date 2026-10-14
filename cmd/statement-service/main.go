// statement-service é o consumidor (destino, serviço de extrato). Lê as mudanças de contas e
// transações da fila e as materializa no banco de destino. A lógica de aplicação é a mesma
// nos dois protótipos; só muda o formato de entrada:
//
//   - domain-events: um evento por operação de negócio no tópico
//     paysim.domain-events.transactions, com a transação e as contas tocadas;
//   - cdc: uma mensagem por linha alterada nos tópicos paysim.cdc.public.<tabela>,
//     já achatada pelo ExtractNewRecordState do Debezium.
//
// Cada entidade recebida é registrada em received_events (inclusive duplicadas e fora de
// ordem) na mesma transação em que é aplicada, o que permite medir perda, duplicação e ordem
// depois.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/pg"
)

// change é a unidade que o consumidor aplica, independente do modelo de propagação.
type change struct {
	eventType   string
	transaction *model.Transaction
	accounts    []model.Account
}

type consumer struct {
	mode string
	db   *pgxpool.Pool

	consumed, applied, invalid, dbErrors *atomic.Int64
}

func main() {
	mode := config.Mode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workers := config.Int("CONSUMER_WORKERS", 3)
	db := pg.Connect(ctx, config.String("DATABASE_URL", "postgres://paysim:paysim@postgres-destino:5432/paysim"),
		int32(workers+2))
	defer db.Close()

	defaultTopics := "paysim.domain-events.transactions"
	if mode == config.ModeCDC {
		defaultTopics = "paysim.cdc.public.transactions,paysim.cdc.public.accounts"
	}
	topics := config.List("CONSUMER_TOPICS", defaultTopics)
	groupID := config.String("CONSUMER_GROUP_ID", "statement-service-"+mode)
	brokers := config.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")

	reg := metrics.New()
	c := &consumer{
		mode:     mode,
		db:       db,
		consumed: reg.Counter("messages_consumed_total", "mensagens lidas da fila"),
		applied:  reg.Counter("messages_applied_total", "mensagens que alteraram a projeção"),
		invalid:  reg.Counter("messages_invalid_total", "mensagens que não puderam ser decodificadas"),
		dbErrors: reg.Counter("db_errors_total", "falhas ao gravar no banco de destino (com nova tentativa)"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"status":"ok","mode":%q}`, mode)
	})
	mux.Handle("GET /metrics", reg.Handler())
	go func() {
		addr := config.String("HTTP_ADDR", ":8081")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("statement-service consumindo %v (grupo %s, %d workers, modo %s)", topics, groupID, workers, mode)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokers,
				GroupID:        groupID,
				GroupTopics:    topics,
				MinBytes:       1,
				MaxBytes:       10 << 20,
				MaxWait:        500 * time.Millisecond,
				StartOffset:    kafka.FirstOffset,
				CommitInterval: config.Duration("CONSUMER_COMMIT_INTERVAL", time.Second),
				SessionTimeout: config.Duration("CONSUMER_SESSION_TIMEOUT", 10*time.Second),
				ErrorLogger:    kafka.LoggerFunc(throttledLogger(fmt.Sprintf("worker %d: ", id))),
			})
			defer r.Close()
			c.run(ctx, r)
		}(i)
	}
	wg.Wait()
}

func (c *consumer) run(ctx context.Context, r *kafka.Reader) {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("erro lendo a fila: %v", err)
			time.Sleep(time.Second)
			continue
		}
		c.consumed.Add(1)
		c.handle(ctx, msg)
		// Commit assíncrono (CommitInterval): entrega pelo menos uma vez.
		if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			log.Printf("erro registrando offset: %v", err)
		}
	}
}

func (c *consumer) handle(ctx context.Context, msg kafka.Message) {
	if len(msg.Value) == 0 {
		return // tombstone
	}
	ch, err := c.decode(msg)
	if err != nil {
		c.invalid.Add(1)
		log.Printf("mensagem inválida em %s/%d@%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
		return
	}
	// Tenta até conseguir gravar: o offset só avança depois do registro no destino.
	for attempt := 0; ; attempt++ {
		err = c.apply(ctx, msg, ch)
		if err == nil || ctx.Err() != nil {
			return
		}
		c.dbErrors.Add(1)
		if attempt%10 == 0 {
			log.Printf("erro gravando no destino (tentativa %d): %v", attempt+1, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *consumer) decode(msg kafka.Message) (change, error) {
	if c.mode == config.ModeDomainEvents {
		var ev model.DomainEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			return change{}, err
		}
		if ev.Transaction.TransactionID == "" {
			return change{}, errors.New("evento sem transação")
		}
		// A correlação do envelope vale para todas as entidades do evento.
		corr := ev.CorrelationID
		ev.Transaction.CorrelationID = &corr
		for i := range ev.Accounts {
			ev.Accounts[i].LastCorrelationID = &corr
		}
		return change{eventType: ev.EventType, transaction: &ev.Transaction, accounts: ev.Accounts}, nil
	}

	// CDC: a mensagem é a linha após a mudança, com os campos __op e __table adicionados
	// pelo ExtractNewRecordState.
	var meta struct {
		Op    string `json:"__op"`
		Table string `json:"__table"`
	}
	if err := json.Unmarshal(msg.Value, &meta); err != nil {
		return change{}, err
	}
	table := meta.Table
	if table == "" {
		table = msg.Topic[strings.LastIndex(msg.Topic, ".")+1:]
	}
	eventType := "row." + meta.Op
	switch table {
	case "transactions":
		var t model.Transaction
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			return change{}, err
		}
		return change{eventType: eventType, transaction: &t}, nil
	case "accounts":
		var a model.Account
		if err := json.Unmarshal(msg.Value, &a); err != nil {
			return change{}, err
		}
		return change{eventType: eventType, accounts: []model.Account{a}}, nil
	}
	return change{}, errors.New("tabela desconhecida: " + table)
}

// receipt é uma linha de received_events.
type receipt struct {
	entity, key string
	version     *int64
	corr        *string
	applied     bool
}

// apply grava a mudança e os recibos na mesma transação do banco de destino. A conta só é
// sobrescrita por uma versão maior (guarda de versão), e a transação só é gravada uma vez, de
// modo que duplicatas e mensagens atrasadas ficam registradas mas não regridem a projeção.
func (c *consumer) apply(ctx context.Context, msg kafka.Message, ch change) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	var receipts []receipt
	if t := ch.transaction; t != nil {
		tag, err := tx.Exec(ctx, `
			INSERT INTO transactions (transaction_id, step, type, amount, name_orig, name_dest,
				old_balance_orig, new_balance_orig, old_balance_dest, new_balance_dest,
				is_fraud, is_flagged_fraud, correlation_id, created_at)
			VALUES ($1, $2, $3, $4::text::numeric, $5, $6, $7::text::numeric, $8::text::numeric,
				$9::text::numeric, $10::text::numeric, $11, $12, $13, $14)
			ON CONFLICT (transaction_id) DO NOTHING`,
			t.TransactionID, t.Step, t.Type, t.Amount, t.NameOrig, t.NameDest, t.OldBalanceOrig, t.NewBalanceOrig,
			t.OldBalanceDest, t.NewBalanceDest, t.IsFraud, t.IsFlaggedFraud, t.CorrelationID, t.CreatedAt)
		if err != nil {
			return err
		}
		receipts = append(receipts, receipt{entity: "transaction", key: t.TransactionID, corr: t.CorrelationID,
			applied: tag.RowsAffected() > 0})
	}
	for _, a := range ch.accounts {
		tag, err := tx.Exec(ctx, `
			INSERT INTO accounts (account_id, account_type, balance, version, last_transaction_id,
				last_correlation_id, updated_at)
			VALUES ($1, $2, $3::text::numeric, $4, $5, $6, $7)
			ON CONFLICT (account_id) DO UPDATE SET
				account_type = EXCLUDED.account_type, balance = EXCLUDED.balance, version = EXCLUDED.version,
				last_transaction_id = EXCLUDED.last_transaction_id,
				last_correlation_id = EXCLUDED.last_correlation_id,
				updated_at = EXCLUDED.updated_at, applied_at = clock_timestamp()
			WHERE accounts.version < EXCLUDED.version`,
			a.AccountID, a.AccountType, a.Balance, a.Version, a.LastTransactionID, a.LastCorrelationID, a.UpdatedAt)
		if err != nil {
			return err
		}
		v := a.Version
		receipts = append(receipts, receipt{entity: "account", key: a.AccountID, version: &v,
			corr: a.LastCorrelationID, applied: tag.RowsAffected() > 0})
	}

	var kafkaTS *time.Time
	if !msg.Time.IsZero() {
		t := msg.Time.UTC()
		kafkaTS = &t
	}
	batch := &pgx.Batch{}
	applied := false
	for _, r := range receipts {
		applied = applied || r.applied
		batch.Queue(`
			INSERT INTO received_events (source, topic, kafka_partition, kafka_offset, kafka_timestamp,
				event_type, entity, entity_key, version, correlation_id, applied)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			c.mode, msg.Topic, msg.Partition, msg.Offset, kafkaTS, ch.eventType, r.entity, r.key, r.version,
			r.corr, r.applied)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if applied {
		c.applied.Add(1)
	}
	return nil
}

// throttledLogger evita inundar o log com a mesma falha de conexão enquanto os brokers
// estão fora: registra no máximo uma linha a cada 5 s por worker.
func throttledLogger(prefix string) func(string, ...interface{}) {
	var mu sync.Mutex
	var last time.Time
	var suppressed int
	return func(format string, args ...interface{}) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(last) < 5*time.Second {
			suppressed++
			return
		}
		msg := fmt.Sprintf(format, args...)
		if suppressed > 0 {
			msg += fmt.Sprintf(" (+%d mensagens suprimidas)", suppressed)
		}
		log.Print(prefix + msg)
		last, suppressed = time.Now(), 0
	}
}
