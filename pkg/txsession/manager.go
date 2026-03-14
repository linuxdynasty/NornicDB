package txsession

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

// ExecutorFactory creates a fresh executor scoped to a database.
// Each explicit transaction gets its own executor instance.
type ExecutorFactory func(dbName string) (*cypher.StorageExecutor, error)

// Session stores explicit transaction state shared across transports.
type Session struct {
	ID       string
	Database string
	Owner    string
	Executor *cypher.StorageExecutor
	Expires  time.Time

	mu sync.Mutex
}

// Manager tracks explicit transaction sessions and lifecycle operations.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session

	ttl     time.Duration
	nowFunc func() time.Time
	idFunc  func() string

	factory ExecutorFactory
}

func NewManager(ttl time.Duration, factory ExecutorFactory) *Manager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Manager{
		sessions: make(map[string]*Session),
		ttl:      ttl,
		nowFunc:  time.Now,
		idFunc: func() string {
			return strconv.FormatInt(time.Now().UnixNano(), 10)
		},
		factory: factory,
	}
}

func (m *Manager) Open(ctx context.Context, dbName string) (*Session, error) {
	return m.OpenForOwner(ctx, dbName, "")
}

func (m *Manager) OpenForOwner(ctx context.Context, dbName string, owner string) (*Session, error) {
	if m.factory == nil {
		return nil, fmt.Errorf("transaction manager factory is not configured")
	}
	executor, err := m.factory(dbName)
	if err != nil {
		return nil, err
	}

	return m.OpenWithExecutorForOwner(ctx, dbName, executor, owner)
}

// OpenWithExecutor opens a transaction session using a pre-built executor.
// This is used when executor construction depends on request context (for example
// forwarding auth to remote composite constituents).
func (m *Manager) OpenWithExecutor(ctx context.Context, dbName string, executor *cypher.StorageExecutor) (*Session, error) {
	return m.OpenWithExecutorForOwner(ctx, dbName, executor, "")
}

func (m *Manager) OpenWithExecutorForOwner(ctx context.Context, dbName string, executor *cypher.StorageExecutor, owner string) (*Session, error) {
	if executor == nil {
		return nil, fmt.Errorf("transaction executor is not available")
	}

	if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}

	now := m.nowFunc()
	session := &Session{
		ID:       m.idFunc(),
		Database: dbName,
		Owner:    strings.TrimSpace(owner),
		Executor: executor,
		Expires:  now.Add(m.ttl),
	}

	m.mu.Lock()
	m.sessions[session.ID] = session
	m.mu.Unlock()

	return session, nil
}

func (m *Manager) Get(txID string) (*Session, bool) {
	return m.GetForOwner(txID, "")
}

func (m *Manager) GetForOwner(txID string, owner string) (*Session, bool) {
	m.mu.RLock()
	session, ok := m.sessions[txID]
	m.mu.RUnlock()
	if !ok || session == nil {
		return nil, false
	}
	boundOwner := strings.TrimSpace(session.Owner)
	if boundOwner == "" {
		return session, true
	}
	if strings.TrimSpace(owner) == "" {
		return nil, false
	}
	if boundOwner != strings.TrimSpace(owner) {
		return nil, false
	}
	return session, ok
}

func (m *Manager) Delete(txID string) {
	m.mu.Lock()
	delete(m.sessions, txID)
	m.mu.Unlock()
}

func (m *Manager) Touch(session *Session) {
	if session == nil {
		return
	}
	session.Expires = m.nowFunc().Add(m.ttl)
}

func (m *Manager) ExecuteInSession(ctx context.Context, session *Session, query string, params map[string]interface{}) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	defer session.mu.Unlock()

	result, err := session.Executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	m.Touch(session)
	return result, nil
}

func (m *Manager) CommitAndDelete(ctx context.Context, session *Session) (*cypher.ExecuteResult, error) {
	if session == nil || session.Executor == nil {
		return nil, fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	defer m.Delete(session.ID)

	return session.Executor.Execute(ctx, "COMMIT", nil)
}

func (m *Manager) RollbackAndDelete(ctx context.Context, session *Session) error {
	if session == nil || session.Executor == nil {
		return fmt.Errorf("transaction session is not available")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	defer m.Delete(session.ID)

	_, err := session.Executor.Execute(ctx, "ROLLBACK", nil)
	return err
}
