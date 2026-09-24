package logger

import (
	"testing"
)

func TestLoggerInitializes(t *testing.T) {
	logger := New(true)
	if logger == nil {
		t.Error("Expected logger to be initialized")
	}
}
