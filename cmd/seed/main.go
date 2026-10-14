// seed faz a carga inicial dos bancos a partir do CSV do dataset PaySim.
//
// As linhas do CSV, que já vêm em ordem de step (hora simulada), recebem um identificador
// sequencial (T000000001, T000000002, ...). Das primeiras PAYSIM_MAX_ROWS linhas (0 = todas),
// a primeira fração (SEED_FRACTION, padrão 0.7) é gravada diretamente nas bases de origem e de
// destino, que partem assim do mesmo estado. A fração restante não é tocada aqui: ela é
// reproduzida como carga transacional pela API (scripts/carga), com a mesma regra de corte
// implementada em scripts/comum/paysim.py.
//
// Os saldos seguem a mesma regra do transaction-service: a conta nasce com o saldo antigo
// informado na primeira linha em que aparece e cada transação aplica o valor (CASH_IN credita
// a origem e debita o destino; os demais tipos fazem o contrário). A versão da conta é o
// número de transações que a tocaram.
//
// A carga é idempotente: se a origem já tiver transações, nada é feito.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software2/internal/pg"
)

// Nome do arquivo no Kaggle (ealaxi/paysim1) e marcador da amostra sintética, gravado por
// scripts/dados/gerar_amostra_sintetica.py.
const (
	kaggleFile      = "PS_20174392719_1491204439457_log.csv"
	syntheticMarker = "_SINTETICO"
)

// Colunas do CSV original. "oldbalanceOrg" é grafado assim no dataset.
var columns = []string{"step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
	"nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"}

type account struct {
	balance int64 // centavos
	version int64
	lastTx  int64
}

func main() {
	ctx := context.Background()
	dir := config.String("PAYSIM_DATA_DIR", "/data")
	fraction, err := strconv.ParseFloat(config.String("SEED_FRACTION", "0.7"), 64)
	if err != nil || fraction < 0 || fraction > 1 {
		log.Fatalf("SEED_FRACTION deve estar entre 0 e 1")
	}
	maxRows := config.Int("PAYSIM_MAX_ROWS", 0)
	path := dataFile(dir)
	_, statErr := os.Stat(filepath.Join(dir, syntheticMarker))
	synthetic := statErr == nil
	if synthetic {
		log.Printf("ATENÇÃO: %s contém a amostra SINTÉTICA, não o dataset real do PaySim", dir)
	}

	origin := pg.Connect(ctx, config.String("ORIGIN_DATABASE_URL", "postgres://paysim:paysim@postgres-origem:5432/paysim"), 4)
	defer origin.Close()
	dest := pg.Connect(ctx, config.String("DESTINATION_DATABASE_URL", "postgres://paysim:paysim@postgres-destino:5432/paysim"), 4)
	defer dest.Close()

	var existing int
	if err := origin.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&existing); err != nil {
		log.Fatal(err)
	}
	if existing > 0 {
		log.Printf("origem já possui %d transações; carga inicial ignorada", existing)
		return
	}

	start := time.Now()
	total := countRows(path)
	if maxRows > 0 && maxRows < total {
		total = maxRows
	}
	cut := int(math.Floor(float64(total) * fraction))
	log.Printf("%s: %d transações consideradas (PAYSIM_MAX_ROWS=%d); %d (%.0f%%) entram na carga inicial",
		filepath.Base(path), total, maxRows, cut, fraction*100)

	tmp, err := os.MkdirTemp("", "seed")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	txFile, accFile := filepath.Join(tmp, "transactions.csv"), filepath.Join(tmp, "accounts.csv")
	accounts := replay(path, cut, txFile)
	writeAccounts(accounts, accFile)
	log.Printf("%d transações e %d contas preparadas em %s", cut, len(accounts), time.Since(start).Round(time.Millisecond))

	for _, db := range []*pgxpool.Pool{origin, dest} {
		copyFile(ctx, db, accFile, "accounts", "account_id, account_type, balance, version, last_transaction_id")
		copyFile(ctx, db, txFile, "transactions", "transaction_id, step, type, amount, name_orig, name_dest, "+
			"old_balance_orig, new_balance_orig, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud")
	}

	if _, err := origin.Exec(ctx, `INSERT INTO seed_info (seed_fraction, data_file, transactions_total,
		transactions_seeded, accounts_seeded, synthetic) VALUES ($1, $2, $3, $4, $5, $6)`,
		fraction, filepath.Base(path), total, cut, len(accounts), synthetic); err != nil {
		log.Fatal(err)
	}
	for _, db := range []*pgxpool.Pool{origin, dest} {
		if _, err := db.Exec(ctx, `ANALYZE`); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("carga inicial concluída em %s", time.Since(start).Round(time.Millisecond))
}

// dataFile acha o CSV: PAYSIM_FILE, senão o nome do Kaggle, senão o único .csv da pasta.
func dataFile(dir string) string {
	if f := config.String("PAYSIM_FILE", ""); f != "" {
		return filepath.Join(dir, f)
	}
	if _, err := os.Stat(filepath.Join(dir, kaggleFile)); err == nil {
		return filepath.Join(dir, kaggleFile)
	}
	found, _ := filepath.Glob(filepath.Join(dir, "*.csv"))
	if len(found) == 1 {
		return found[0]
	}
	log.Fatalf("CSV do PaySim não encontrado em %s (achados: %d). Baixe o dataset do Kaggle e rode "+
		"scripts/dados/preparar_paysim.py (ver README), gere a amostra sintética com "+
		"scripts/dados/gerar_amostra_sintetica.py ou indique o arquivo em PAYSIM_FILE.", dir, len(found))
	return ""
}

// countRows conta as linhas de dados (sem o cabeçalho).
func countRows(path string) int {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	n, last := 0, byte('\n')
	for {
		k, err := f.Read(buf)
		n += bytes.Count(buf[:k], []byte{'\n'})
		if k > 0 {
			last = buf[k-1]
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if last != '\n' {
		n++ // última linha sem quebra
	}
	return n - 1
}

// replay percorre as primeiras `cut` linhas aplicando a regra de saldo e grava as transações
// num CSV pronto para o COPY. Devolve o estado final das contas.
func replay(path string, cut int, out string) map[string]*account {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	r.ReuseRecord = true
	header, err := r.Read()
	if err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimPrefix(strings.TrimSpace(h), "\xef\xbb\xbf")] = i
	}
	for _, c := range columns {
		if _, ok := idx[c]; !ok {
			log.Fatalf("%s: coluna %s ausente (esperado o esquema do PaySim: %s)", path, c, strings.Join(columns, ","))
		}
	}

	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	accounts := make(map[string]*account, cut)
	get := func(id string, initial int64) *account {
		a, ok := accounts[id]
		if !ok {
			a = &account{balance: initial}
			accounts[id] = a
		}
		return a
	}
	for i := 0; i < cut; i++ {
		rec, err := r.Read()
		if err != nil {
			log.Fatalf("%s: linha %d: %v", path, i+2, err)
		}
		col := func(c string) string { return rec[idx[c]] }
		typ := col("type")
		if !model.TransactionTypes[typ] {
			log.Fatalf("%s: linha %d: tipo desconhecido %q", path, i+2, typ)
		}
		amount := cents(col("amount"), i)
		orig, dest := col("nameOrig"), col("nameDest")
		origDelta, destDelta := -amount, amount
		if model.CreditsOrigin(typ) {
			origDelta, destDelta = amount, -amount
		}
		o := get(orig, cents(col("oldbalanceOrg"), i))
		oldO := o.balance
		var oldD int64
		d := o
		if orig == dest {
			o.version++
		} else {
			d = get(dest, cents(col("oldbalanceDest"), i))
			oldD = d.balance
			o.balance += origDelta
			d.balance += destDelta
			o.version++
			d.version++
		}
		if orig == dest {
			oldD = oldO
		}
		o.lastTx, d.lastTx = int64(i+1), int64(i+1)
		fmt.Fprintf(bw, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%t,%t\n", txID(i+1), col("step"), typ, money(amount),
			orig, dest, money(oldO), money(o.balance), money(oldD), money(d.balance),
			col("isFraud") == "1", col("isFlaggedFraud") == "1")
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
	return accounts
}

func writeAccounts(accounts map[string]*account, out string) {
	ids := make([]string, 0, len(accounts))
	for id := range accounts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	for _, id := range ids {
		a := accounts[id]
		typ := "C"
		if strings.HasPrefix(id, "M") {
			typ = "M"
		}
		fmt.Fprintf(bw, "%s,%s,%s,%d,%s\n", id, typ, money(a.balance), a.version, txID(int(a.lastTx)))
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
}

func copyFile(ctx context.Context, db *pgxpool.Pool, path, table, cols string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	conn, err := db.Acquire(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, f, fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT csv)", table, cols))
	if err != nil {
		log.Fatalf("copiando %s: %v", table, err)
	}
	log.Printf("%-13s %9d linhas (%s)", table, tag.RowsAffected(), db.Config().ConnConfig.Host)
}

// txID é o identificador da transação a partir da posição da linha no CSV (1 = primeira).
// Precisa coincidir com scripts/comum/paysim.py.
func txID(n int) string { return fmt.Sprintf("T%09d", n) }

func cents(v string, line int) int64 {
	if v == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("linha %d: valor inválido %q", line+2, v)
	}
	return int64(math.Round(f * 100))
}

func money(c int64) string {
	sign := ""
	if c < 0 {
		sign, c = "-", -c
	}
	return fmt.Sprintf("%s%d.%02d", sign, c/100, c%100)
}
