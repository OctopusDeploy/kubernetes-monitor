package crypto

import (
	"crypto/sha256"
	"encoding/base64"
)

type HashSalt string

func HashString(hashSalt HashSalt, value string) string {
	if hashSalt == "" {
		panic("The hashing salt is empty")
	}

	h := sha256.New()
	h.Write([]byte(hashSalt))
	h.Write([]byte(value))

	hashed := h.Sum(nil)
	sha := base64.StdEncoding.EncodeToString(hashed)
	return sha
}
