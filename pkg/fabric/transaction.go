package fabric

import (
	"fmt"
	"sync"
)

// ErrSecondWriteShard is returned when a distributed transaction attempts to write
// to a second shard. Neo4j Fabric enforces a many-read/one-write constraint per
// transaction — this error matches that contract with a stable code and message.
var ErrSecondWriteShard = fmt.Errorf("Neo.ClientError.Transaction.ForbiddenDueToTransactionType: " +
	"Writing to more than one database per transaction is not allowed")

// SubTransaction represents an open sub-transaction on a single shard.
type SubTransaction struct {
	// ShardName identifies the constituent (e.g. "caremark.tr").
	ShardName string

	// IsWrite is true if any write has been performed on this shard.
	IsWrite bool

	// State tracks the lifecycle: "open", "committed", "rolledback".
	State string
}

// FabricTransaction coordinates sub-transactions across participating shards
// within a single distributed transaction.
//
// Invariants enforced:
//   - Reads may span any number of shards.
//   - At most one shard may receive write operations per transaction.
//   - Attempting a write on a second shard is rejected deterministically.
//   - Commit commits all open sub-transactions; rollback rolls back all.
//
// This mirrors Neo4j's FabricTransaction.java.
type FabricTransaction struct {
	subTxns    map[string]*SubTransaction
	writeShard string // name of the single shard that has writes, or ""
	mu         sync.Mutex
	state      string // "open", "committed", "rolledback"
	txID       string
}

// NewFabricTransaction creates a new distributed transaction.
func NewFabricTransaction(txID string) *FabricTransaction {
	return &FabricTransaction{
		subTxns: make(map[string]*SubTransaction),
		state:   "open",
		txID:    txID,
	}
}

// TxID returns the transaction identifier.
func (t *FabricTransaction) TxID() string {
	return t.txID
}

// State returns the current transaction state.
func (t *FabricTransaction) State() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// WriteShard returns the name of the shard that holds the write lock,
// or empty string if no writes have been performed.
func (t *FabricTransaction) WriteShard() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writeShard
}

// GetOrOpen returns an existing sub-transaction for the shard, or opens a new one.
// If isWrite is true and a different shard already holds the write lock,
// ErrSecondWriteShard is returned.
func (t *FabricTransaction) GetOrOpen(shardName string, isWrite bool) (*SubTransaction, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return nil, fmt.Errorf("transaction is %s", t.state)
	}

	// Check write constraint before anything else.
	if isWrite && t.writeShard != "" && t.writeShard != shardName {
		return nil, ErrSecondWriteShard
	}

	sub, exists := t.subTxns[shardName]
	if !exists {
		sub = &SubTransaction{
			ShardName: shardName,
			IsWrite:   isWrite,
			State:     "open",
		}
		t.subTxns[shardName] = sub
	}

	// Upgrade read-only sub-transaction to write if needed.
	if isWrite && !sub.IsWrite {
		if t.writeShard != "" && t.writeShard != shardName {
			return nil, ErrSecondWriteShard
		}
		sub.IsWrite = true
	}

	// Track write shard.
	if isWrite {
		t.writeShard = shardName
	}

	return sub, nil
}

// Participants returns the names of all shards participating in this transaction.
func (t *FabricTransaction) Participants() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	names := make([]string, 0, len(t.subTxns))
	for name := range t.subTxns {
		names = append(names, name)
	}
	return names
}

// SubTransactions returns a snapshot of all sub-transactions.
func (t *FabricTransaction) SubTransactions() map[string]*SubTransaction {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]*SubTransaction, len(t.subTxns))
	for k, v := range t.subTxns {
		copy := *v
		result[k] = &copy
	}
	return result
}

// CommitCallback is called for each sub-transaction during commit.
// The callback should perform the actual commit on the shard.
// If any callback returns an error, commit halts and the remaining
// sub-transactions are rolled back via RollbackCallback.
type CommitCallback func(sub *SubTransaction) error

// RollbackCallback is called for each sub-transaction during rollback.
type RollbackCallback func(sub *SubTransaction) error

// Commit commits all open sub-transactions using the provided callback.
// On partial failure, remaining sub-transactions are rolled back.
func (t *FabricTransaction) Commit(commitFn CommitCallback, rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return fmt.Errorf("cannot commit: transaction is %s", t.state)
	}

	// Commit all sub-transactions. On failure, rollback the rest.
	var committed []*SubTransaction
	var commitErr error

	for _, sub := range t.subTxns {
		if sub.State != "open" {
			continue
		}
		if err := commitFn(sub); err != nil {
			commitErr = fmt.Errorf("commit failed on shard '%s': %w", sub.ShardName, err)
			break
		}
		sub.State = "committed"
		committed = append(committed, sub)
	}

	if commitErr != nil {
		// Rollback uncommitted sub-transactions.
		for _, sub := range t.subTxns {
			if sub.State == "open" {
				if rollbackFn != nil {
					_ = rollbackFn(sub)
				}
				sub.State = "rolledback"
			}
		}
		t.state = "rolledback"
		return commitErr
	}

	t.state = "committed"
	return nil
}

// Rollback rolls back all open sub-transactions using the provided callback.
func (t *FabricTransaction) Rollback(rollbackFn RollbackCallback) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != "open" {
		return fmt.Errorf("cannot rollback: transaction is %s", t.state)
	}

	var lastErr error
	for _, sub := range t.subTxns {
		if sub.State != "open" {
			continue
		}
		if rollbackFn != nil {
			if err := rollbackFn(sub); err != nil {
				lastErr = fmt.Errorf("rollback failed on shard '%s': %w", sub.ShardName, err)
			}
		}
		sub.State = "rolledback"
	}

	t.state = "rolledback"
	return lastErr
}
