package cluster

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func generateBytes(numBytes int) []byte {
	token := make([]byte, numBytes)
	_, _ = rand.Read(token)
	return token
}

func bytesEqual(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}

func TestBufferReturnsAllData_WhenFilledPartially(t *testing.T) {
	testData := generateBytes(5)
	buffer := NewCircularBuffer(10)
	buffer.Write(testData)

	actualBytes := buffer.Bytes()
	expectedBytes := testData
	if !bytesEqual(expectedBytes, actualBytes) {
		t.Errorf("Expected %v, got %v", actualBytes, expectedBytes)
	}
}

func TestBufferReturnsAllData_WhenFilledCompletely(t *testing.T) {
	testData := generateBytes(10)
	buffer := NewCircularBuffer(10)
	buffer.Write(testData)

	actualBytes := buffer.Bytes()
	expectedBytes := testData
	if !bytesEqual(expectedBytes, actualBytes) {
		t.Errorf("Expected %v, got %v", actualBytes, expectedBytes)
	}
}

func TestBufferReturnsAllData_WhenFilledInChunks(t *testing.T) {
	testData := generateBytes(5)
	buffer := NewCircularBuffer(10)
	buffer.Write(testData)
	buffer.Write(testData)

	actualBytes := buffer.Bytes()
	expectedBytes := append(testData, testData...)
	if !bytesEqual(expectedBytes, actualBytes) {
		t.Errorf("Expected %v, got %v", actualBytes, expectedBytes)
	}
}

func TestBufferReturnsOnlyCapacity_WhenOverFilled(t *testing.T) {
	testData := generateBytes(9)
	buffer := NewCircularBuffer(3)
	buffer.Write(testData)

	actualBytes := buffer.Bytes()
	expectedBytes := testData[6:]
	if !bytesEqual(expectedBytes, actualBytes) {
		t.Errorf("Expected %v, got %v", actualBytes, expectedBytes)
	}
}
