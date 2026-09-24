package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashString(t *testing.T) {
	expected := "rf+ELuSDCCMNp0r5dtKW94mL9MH0vFqRpxhGTWpa//k="

	salt := HashSalt("Projects-123/Environments-45/Tenants-6")
	actual := HashString(salt, "secret-value你好")

	if expected != actual {
		t.Errorf("Expected hashed string to be to be '%v', but got '%v'", expected, actual)
	}
}

func TestHashStringWithInvalidSalt_Panics(t *testing.T) {
	salt := HashSalt("")

	assert.Panics(t, func() { HashString(salt, "secret-value你好") })
}
