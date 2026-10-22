package binlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// PositionStore persists the last fully delivered binlog position.
type PositionStore interface {
	Load() (mysql.Position, error)
	Save(mysql.Position) error
}

type storedPosition struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

// FileStore keeps the position in a JSON file, replaced atomically on every
// save so a crash mid-write cannot leave a truncated offset behind.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore returns a store backed by path, creating its parent directory.
func NewFileStore(path string) (*FileStore, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &FileStore{path: path}, nil
}

// Load returns the stored position, or the zero position when nothing has been
// committed yet.
func (f *FileStore) Load() (mysql.Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	raw, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return mysql.Position{}, nil
	}
	if err != nil {
		return mysql.Position{}, err
	}

	var sp storedPosition
	if err := json.Unmarshal(raw, &sp); err != nil {
		return mysql.Position{}, fmt.Errorf("position file %s is corrupt: %w", f.path, err)
	}
	return mysql.Position{Name: sp.File, Pos: sp.Pos}, nil
}

// Save writes pos via a temporary file and a rename.
func (f *FileStore) Save(pos mysql.Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	raw, err := json.Marshal(storedPosition{File: pos.Name, Pos: pos.Pos})
	if err != nil {
		return err
	}

	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

// MemoryStore is an in-memory PositionStore, used by the tests.
type MemoryStore struct {
	mu  sync.Mutex
	pos mysql.Position
}

func (m *MemoryStore) Load() (mysql.Position, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pos, nil
}

func (m *MemoryStore) Save(pos mysql.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pos = pos
	return nil
}
