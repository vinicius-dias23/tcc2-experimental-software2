// transaction-service é o serviço de negócio (origem). Recebe as transações financeiras do
// PaySim, atualiza os saldos das contas de origem e de destino e grava a transação, tudo na
// mesma transação do banco de origem. Apenas no modo domain-events (Software A) publica o
// evento de domínio diretamente na fila logo após o commit. Se essa publicação falhar, o
// evento vai para uma fila de retentativa em memória e, esgotadas as tentativas, é declarado
// perdido.
//
// No modo cdc (Software B) o código é o mesmo, mas a publicação é desligada: quem propaga é o
// Debezium, lendo o WAL do banco de origem.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/events"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/pg"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/retry"
)

const accountColumns = `account_id, account_type, balance::text, version, last_transaction_id,
	last_correlation_id::text, updated_at`

const transactionColumns = `transaction_id, step, type, amount::text, name_orig, name_dest,
	old_balance_orig::text, new_balance_orig::text, old_balance_dest::text, new_balance_dest::text,
	is_fraud, is_flagged_fraud, correlation_id::text, created_at`

type server struct {
	mode string
	db   *pgxpool.Pool
	pub  events.Publisher
	// Retentativa em memória dos eventos cuja publicação síncrona falhou (Software A).
	retry *events.RetryQueue
	// Contexto do processo: as retentativas não dependem da requisição que as originou.
	ctx context.Context
	// Resposta ao cliente quando a publicação falha depois do commit:
	// "success" (padrão) responde 2xx, pois a mudança está commitada;
	// "error" responde 503, embora o commit já tenha ocorrido.
	failureResponse string
	// Tempo máximo que a requisição espera a confirmação do broker; esgotado, a publicação
	// é dada como falha na resposta e o resultado real fica com a fila de retentativa.
	publishTimeout time.Duration
	// Tempo máximo das operações no banco por requisição; esgotado, a API responde 503.
	dbTimeout time.Duration

	created, httpErrors      *atomic.Int64
	published, publishFailed *atomic.Int64
	publishLatencyMs         *atomic.Int64
	failuresSinceLog         atomic.Int64
}

func main() {
	mode := config.Mode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := pg.Connect(ctx, config.String("DATABASE_URL", "postgres://paysim:paysim@postgres-origem:5432/paysim"),
		int32(config.Int("DB_MAX_CONNS", 30)))
	defer db.Close()

	reg := metrics.New()
	s := &server{
		mode:             mode,
		db:               db,
		ctx:              ctx,
		failureResponse:  config.String("PUBLISH_FAILURE_RESPONSE", "success"),
		dbTimeout:        config.Duration("DB_TIMEOUT", 10*time.Second),
		created:          reg.Counter("transactions_created_total", "transações commitadas"),
		httpErrors:       reg.Counter("http_errors_total", "requisições respondidas com erro"),
		published:        reg.Counter("events_published_total", "eventos de domínio confirmados pelo broker"),
		publishFailed:    reg.Counter("events_publish_failed_total", "eventos de domínio cuja publicação síncrona falhou após o commit (vão para a retentativa)"),
		publishLatencyMs: reg.Counter("events_publish_latency_ms_sum", "soma da latência de publicação (ms)"),
	}

	if mode == config.ModeDomainEvents {
		p := events.NewKafkaPublisher(events.KafkaPublisherConfig{
			Brokers:     config.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
			Topic:       config.String("DOMAIN_EVENTS_TOPIC", "paysim.domain-events.transactions"),
			MaxAttempts: config.Int("PUBLISH_MAX_ATTEMPTS", 10),
		})
		s.pub = p
		s.publishTimeout = config.Duration("PUBLISH_TIMEOUT", 5*time.Second)
		s.retry = events.NewRetryQueue(p, retry.FromEnv("PUBLISH_RETRY"),
			config.Int("PUBLISH_RETRY_BUFFER", 10000), reg)
		go s.retry.LogSummary(ctx)
		go s.logPublishFailures(ctx)
	} else {
		s.pub = events.NoopPublisher{}
	}
	defer s.pub.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /transactions", s.createTransaction)
	mux.HandleFunc("GET /transactions/{id}", s.getTransaction)
	mux.HandleFunc("GET /accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "mode": mode})
	})
	mux.Handle("GET /metrics", reg.Handler())

	addr := config.String("HTTP_ADDR", ":8080")
	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()
	log.Printf("transaction-service ouvindo em %s (modo %s)", addr, mode)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// createTransactionRequest é uma linha do PaySim. Os saldos antigos do CSV só são usados
// como saldo inicial quando a conta ainda não existe na origem; daí em diante o saldo é o
// que o serviço calcula.
type createTransactionRequest struct {
	TransactionID  string  `json:"transaction_id"`
	Step           int     `json:"step"`
	Type           string  `json:"type"`
	Amount         string  `json:"amount"`
	NameOrig       string  `json:"name_orig"`
	NameDest       string  `json:"name_dest"`
	OldBalanceOrig *string `json:"old_balance_orig"`
	OldBalanceDest *string `json:"old_balance_dest"`
	IsFraud        bool    `json:"is_fraud"`
	IsFlaggedFraud bool    `json:"is_flagged_fraud"`
}

func (req *createTransactionRequest) validate() string {
	if req.TransactionID == "" || req.NameOrig == "" || req.NameDest == "" {
		return "transaction_id, name_orig e name_dest são obrigatórios"
	}
	if !model.TransactionTypes[req.Type] {
		return "type deve ser CASH_IN, CASH_OUT, DEBIT, PAYMENT ou TRANSFER"
	}
	if v, err := strconv.ParseFloat(req.Amount, 64); err != nil || v < 0 {
		return "amount deve ser um número não negativo"
	}
	for _, b := range []*string{req.OldBalanceOrig, req.OldBalanceDest} {
		if b != nil {
			if _, err := strconv.ParseFloat(*b, 64); err != nil {
				return "saldos inválidos"
			}
		}
	}
	return ""
}

// accountChange é a alteração de saldo de uma conta dentro da transação.
type accountChange struct {
	id      string
	initial *string // saldo inicial, se a conta for nova
	delta   string
}

func (s *server) createTransaction(w http.ResponseWriter, r *http.Request) {
	var req createTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "corpo inválido: "+err.Error())
		return
	}
	if msg := req.validate(); msg != "" {
		s.fail(w, http.StatusBadRequest, "corpo inválido: "+msg)
		return
	}
	corr := correlationID(r)

	// CASH_IN credita a origem e debita o destino; os demais tipos debitam a origem e
	// creditam o destino. Não há verificação de saldo, para que toda linha do dataset seja
	// commitada (o PaySim tem saldos que não fecham).
	debit, credit := "-"+req.Amount, req.Amount
	origDelta, destDelta := debit, credit
	if model.CreditsOrigin(req.Type) {
		origDelta, destDelta = credit, debit
	}
	changes := []accountChange{{req.NameOrig, req.OldBalanceOrig, origDelta}, {req.NameDest, req.OldBalanceDest, destDelta}}
	if req.NameOrig == req.NameDest {
		changes = []accountChange{{req.NameOrig, req.OldBalanceOrig, "0"}}
	}
	// As contas são travadas sempre na mesma ordem, para que duas transações entre as
	// mesmas contas em sentidos opostos não entrem em impasse.
	sort.Slice(changes, func(i, j int) bool { return changes[i].id < changes[j].id })

	ctx, cancel := context.WithTimeout(r.Context(), s.dbTimeout)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer tx.Rollback(context.Background())

	accounts := make(map[string]*model.Account, 2)
	oldBalance := make(map[string]string, 2)
	for _, c := range changes {
		a := &model.Account{AccountID: c.id}
		var old string
		err := tx.QueryRow(ctx, `
			INSERT INTO accounts (account_id, account_type, balance, version, last_transaction_id,
				last_correlation_id, updated_at)
			VALUES ($1, $2, coalesce($3::text::numeric, 0) + $4::text::numeric, 1, $5, $6, now())
			ON CONFLICT (account_id) DO UPDATE SET
				balance = accounts.balance + $4::text::numeric, version = accounts.version + 1,
				last_transaction_id = $5, last_correlation_id = $6, updated_at = now()
			RETURNING account_type, balance::text, (balance - $4::text::numeric)::text, version,
				last_transaction_id, last_correlation_id::text, updated_at`,
			c.id, accountType(c.id), c.initial, c.delta, req.TransactionID, corr,
		).Scan(&a.AccountType, &a.Balance, &old, &a.Version, &a.LastTransactionID, &a.LastCorrelationID, &a.UpdatedAt)
		if err != nil {
			s.fail(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		accounts[c.id], oldBalance[c.id] = a, old
	}

	orig, dest := accounts[req.NameOrig], accounts[req.NameDest]
	oldOrig, oldDest := oldBalance[req.NameOrig], oldBalance[req.NameDest]
	t := model.Transaction{
		TransactionID: req.TransactionID, Step: req.Step, Type: req.Type, NameOrig: req.NameOrig,
		NameDest: req.NameDest, IsFraud: req.IsFraud, IsFlaggedFraud: req.IsFlaggedFraud,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO transactions (transaction_id, step, type, amount, name_orig, name_dest,
			old_balance_orig, new_balance_orig, old_balance_dest, new_balance_dest,
			is_fraud, is_flagged_fraud, correlation_id, created_at)
		VALUES ($1, $2, $3, $4::text::numeric, $5, $6, $7::text::numeric, $8::text::numeric,
			$9::text::numeric, $10::text::numeric, $11, $12, $13, now())
		RETURNING `+transactionColumns,
		req.TransactionID, req.Step, req.Type, req.Amount, req.NameOrig, req.NameDest,
		oldOrig, orig.Balance, oldDest, dest.Balance, req.IsFraud, req.IsFlaggedFraud, corr,
	).Scan(scanTransaction(&t)...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.fail(w, http.StatusConflict, "transação já existe")
			return
		}
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if err := tx.Commit(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.created.Add(1)

	// A partir daqui a mudança está commitada. No Software A o evento é publicado fora da
	// transação: se a publicação falhar, nada durável registra que ele deveria existir.
	evAccounts := []model.Account{*orig}
	if dest != orig {
		evAccounts = append(evAccounts, *dest)
	}
	published := s.publish(model.DomainEvent{
		EventType: model.EventTransactionCompleted, CorrelationID: corr, AggregateID: req.NameOrig,
		Transaction: t, Accounts: evAccounts,
	})
	s.respondAfterCommit(w, http.StatusCreated, published, map[string]any{
		"transaction_id": t.TransactionID, "correlation_id": corr,
		"version_orig": orig.Version, "version_dest": dest.Version,
		"new_balance_orig": orig.Balance, "new_balance_dest": dest.Balance,
	})
}

func (s *server) getTransaction(w http.ResponseWriter, r *http.Request) {
	var t model.Transaction
	err := s.db.QueryRow(r.Context(), `SELECT `+transactionColumns+` FROM transactions WHERE transaction_id = $1`,
		r.PathValue("id")).Scan(scanTransaction(&t)...)
	s.writeFound(w, err, t, "transação não encontrada")
}

func (s *server) getAccount(w http.ResponseWriter, r *http.Request) {
	var a model.Account
	err := s.db.QueryRow(r.Context(), `SELECT `+accountColumns+` FROM accounts WHERE account_id = $1`,
		r.PathValue("id")).Scan(&a.AccountID, &a.AccountType, &a.Balance, &a.Version, &a.LastTransactionID,
		&a.LastCorrelationID, &a.UpdatedAt)
	s.writeFound(w, err, a, "conta não encontrada")
}

func (s *server) writeFound(w http.ResponseWriter, err error, v any, notFound string) {
	if errors.Is(err, pgx.ErrNoRows) {
		s.fail(w, http.StatusNotFound, notFound)
		return
	}
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// publish envia o evento no modo domain-events e devolve se ele foi confirmado na
// publicação síncrona; se não foi, o evento segue para a fila de retentativa.
// No modo cdc não faz nada e devolve true.
func (s *server) publish(ev model.DomainEvent) bool {
	if s.mode != config.ModeDomainEvents {
		return true
	}
	ev.EventID = uuid.NewString()
	ev.OccurredAt = time.Now().UTC()
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("erro serializando evento: %v", err)
		s.publishFailed.Add(1)
		return false
	}
	start := time.Now()
	headers := map[string]string{"correlation_id": ev.CorrelationID, "event_type": ev.EventType}
	// A publicação não é cancelada se o cliente desistir, nem quando a requisição para de
	// esperar: o kafka-go continua tentando, e o resultado real vai para a retentativa.
	result := make(chan error, 1)
	go func() { result <- s.pub.Publish(s.ctx, ev.AggregateID, payload, headers) }()
	timer := time.NewTimer(s.publishTimeout)
	defer timer.Stop()
	select {
	case err = <-result:
		if err != nil {
			s.retry.Enqueue(s.ctx, ev.AggregateID, payload, headers, nil)
		}
	case <-timer.C:
		err = errors.New("tempo de publicação esgotado")
		s.retry.Enqueue(s.ctx, ev.AggregateID, payload, headers, result)
	}
	s.publishLatencyMs.Add(time.Since(start).Milliseconds())
	if err != nil {
		s.publishFailed.Add(1)
		s.failuresSinceLog.Add(1)
		return false
	}
	s.published.Add(1)
	return true
}

func (s *server) respondAfterCommit(w http.ResponseWriter, status int, published bool, body map[string]any) {
	if !published && s.failureResponse == "error" {
		s.fail(w, http.StatusServiceUnavailable, "mudança commitada, mas o evento não foi publicado")
		return
	}
	writeJSON(w, status, body)
}

// logPublishFailures resume as falhas de publicação a cada 5 s, em vez de uma linha
// por requisição, para não inundar o log durante janelas longas de indisponibilidade.
func (s *server) logPublishFailures(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.failuresSinceLog.Swap(0); n > 0 {
				log.Printf("ERRO: %d eventos falharam na publicação síncrona nos últimos 5s e foram para a retentativa (total %d)", n, s.publishFailed.Load())
			}
		}
	}
}

func (s *server) fail(w http.ResponseWriter, status int, msg string) {
	s.httpErrors.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func scanTransaction(t *model.Transaction) []any {
	return []any{&t.TransactionID, &t.Step, &t.Type, &t.Amount, &t.NameOrig, &t.NameDest,
		&t.OldBalanceOrig, &t.NewBalanceOrig, &t.OldBalanceDest, &t.NewBalanceDest,
		&t.IsFraud, &t.IsFlaggedFraud, &t.CorrelationID, &t.CreatedAt}
}

// accountType é o prefixo do identificador no PaySim: C para clientes, M para comerciantes.
func accountType(id string) string {
	if len(id) > 0 && id[0] == 'M' {
		return "M"
	}
	return "C"
}

func correlationID(r *http.Request) string {
	if v := r.Header.Get("X-Correlation-ID"); v != "" {
		if _, err := uuid.Parse(v); err == nil {
			return v
		}
	}
	return uuid.NewString()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
