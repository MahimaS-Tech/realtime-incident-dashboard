package id

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func New(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return format(prefix, b)
}

func Stable(prefix string, parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(strings.TrimSpace(part)))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return format(prefix, sum[:16])
}

func format(prefix string, b []byte) string {
	if prefix == "" {
		return hex.EncodeToString(b)
	}
	return prefix + "_" + hex.EncodeToString(b)
}
