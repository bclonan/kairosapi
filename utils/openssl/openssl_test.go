package openssl

import (
	"fmt"
	"io/ioutil"
	"sovrin-mind-market/models/generator"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCrypter(t *testing.T) {
	key := []byte(generator.GenerateRandomID(32))
	iv := []byte(generator.GenerateRandomID(16))

	// Initialize new crypter struct. Errors are ignored.
	crypter, _ := NewCrypter(key, iv)

	// Lets encode plaintext using the same key and iv.
	// This will produce the very same result: "RanFyUZSP9u/HLZjyI5zXQ=="
	stripeKey := "sk_test_kqcgtNglf8UIlDtPs7ziTKzOxfasfasfsaf"
	encoded, _ := crypter.Encrypt([]byte(stripeKey))
	decoded, _ := crypter.Decrypt(encoded)
	assert.Equal(t, string(decoded), stripeKey)
	assert.Equal(t, decoded, []byte(stripeKey))
}

func TestLoadKey(t *testing.T) {
	iv, err := ioutil.ReadFile("keys/iv.ssl")
	if err != nil {

	}

	key, err := ioutil.ReadFile("keys/key.ssl")
	if err != nil {

	}

	c, err := NewCrypter(key, iv)
	if err != nil {

	}

	testString := []byte("hello world")
	e, err := c.Encrypt(testString)
	if err != nil {

	}

	d, err := c.Decrypt(e)
	if err != nil {
		panic(string(d) + err.Error())
	}

	assert.Equal(t, d, testString)

}

func TestCrypter_Decrypt(t *testing.T) {
	crypt, err := Init()
	if err != nil {
		panic(err)
	}

	e, err := crypt.Encrypt([]byte("sk_test_kqcgtNglf8UIlDtPs7ziTKzOfasf"))
	d, err := crypt.Decrypt(e)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(d))
	fmt.Println(e, d)
}
