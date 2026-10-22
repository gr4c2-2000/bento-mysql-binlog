package binlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "binlog.pos")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	want := mysql.Position{Name: "mysql-bin.000009", Pos: 123456}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	got, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// A missing file means "never committed anything", not an error: that is how a
// fresh deployment falls back to the current master position.
func TestFileStoreMissingFileIsZeroPosition(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "binlog.pos"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "" || got.Pos != 0 {
		t.Errorf("want the zero position, got %v", got)
	}
}

func TestFileStoreCorruptFileFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binlog.pos")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("want an error for a corrupt position file")
	}
}

// Saves go through a temp file and a rename so a crash cannot leave a
// half-written offset behind.
func TestFileStoreSaveLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "binlog.pos")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := s.Save(mysql.Position{Name: "mysql-bin.000001", Pos: 4}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "binlog.pos" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("want only binlog.pos, got %v", names)
	}
}

func TestFileStoreOverwritesPreviousPosition(t *testing.T) {
	s, err := NewFileStore(filepath.Join(t.TempDir(), "binlog.pos"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := s.Save(mysql.Position{Name: "mysql-bin.000001", Pos: 4}); err != nil {
		t.Fatal(err)
	}
	want := mysql.Position{Name: "mysql-bin.000002", Pos: 999}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
