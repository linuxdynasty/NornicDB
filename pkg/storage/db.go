package storage

import "errors"

const DefaultMaxRetries = 5
const DefaultUpdateRetryLimit = 5

type transactionBeginner interface {
	BeginTransaction() (*BadgerTransaction, error)
}

// DB provides closure-based transaction helpers over a transactional storage engine.
type DB struct {
	engine           transactionBeginner
	maxUpdateRetries int
}

// NewDB wraps a transactional storage engine with closure-based transaction helpers.
func NewDB(engine transactionBeginner) *DB {
	return &DB{
		engine:           engine,
		maxUpdateRetries: DefaultMaxRetries,
	}
}

// SetMaxUpdateRetries overrides the default retry limit used by Update.
func (db *DB) SetMaxUpdateRetries(maxRetries int) {
	if maxRetries <= 0 {
		db.maxUpdateRetries = DefaultMaxRetries
		return
	}
	db.maxUpdateRetries = maxRetries
}

// Begin opens a transaction-scoped snapshot for the closure helpers.
// The current storage transaction implementation always uses an explicit
// BadgerTransaction, so the readWrite flag is reserved for future differentiation.
func (db *DB) Begin(readWrite bool) (*Transaction, error) {
	if db == nil || db.engine == nil {
		return nil, ErrStorageClosed
	}
	return db.engine.BeginTransaction()
}

// Update executes fn in a read-write transaction and retries conflicts with a fresh snapshot.
func (db *DB) Update(fn func(tx *Transaction) error) error {
	if db == nil || db.engine == nil {
		return ErrStorageClosed
	}

	maxRetries := db.maxUpdateRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}

		err = invokeTransactionClosure(tx, fn)
		if err != nil {
			if rollbackErr := rollbackActiveTransaction(tx); rollbackErr != nil {
				return rollbackErr
			}
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue
			}
			return err
		}

		if err := tx.Commit(); err != nil {
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue
			}
			return err
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return ErrConflict
}

// View executes fn in a transaction-scoped snapshot and always rolls it back on exit.
func (db *DB) View(fn func(tx *Transaction) error) error {
	if db == nil || db.engine == nil {
		return ErrStorageClosed
	}

	tx, err := db.Begin(false)
	if err != nil {
		return err
	}

	err = invokeTransactionClosure(tx, fn)
	if rollbackErr := rollbackActiveTransaction(tx); rollbackErr != nil {
		return rollbackErr
	}
	return err
}

func rollbackActiveTransaction(tx *BadgerTransaction) error {
	if tx == nil || tx.Status != TxStatusActive {
		return nil
	}
	return tx.Rollback()
}

func invokeTransactionClosure(tx *BadgerTransaction, fn func(tx *Transaction) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = rollbackActiveTransaction(tx)
			panic(recovered)
		}
	}()

	return fn(tx)
}
