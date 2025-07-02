// test/storage_test.go
package test

import (
    "testing"
    "yuclase/pkg/storage"
)

func TestLogAppend(t *testing.T) {
    log := storage.NewLog()
    err := log.Append([]byte("test message"))
    if err != nil {
        t.Errorf("Failed to append message to log: %v", err)
    }
}

func TestLogRead(t *testing.T) {
    log := storage.NewLog()
    _, err := log.Read(0)
    if err != nil {
        t.Errorf("Failed to read message from log: %v", err)
    }
}
