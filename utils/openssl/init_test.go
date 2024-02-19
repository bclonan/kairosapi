package openssl

import (
	"fmt"
	"testing"
)

func TestInit2(t *testing.T) {
	crypter, err := Init()
	if err != nil {
		panic(err)
	}
	k, err := crypter.Encrypt([]byte("sk_stripe"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("encrypted key: %v\n", string(k))
}
