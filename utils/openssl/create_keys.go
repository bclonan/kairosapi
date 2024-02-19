package openssl

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
)

type SSLIV = string
type SSLKey = string

func CreateKeys() {
	saveSSLIV("keys/key.ssl", generateRandomID(32))
	saveSSLKey("keys/iv.ssl", generateRandomID(16))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func generateRandomID(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func saveSSLKey(fileName string, key SSLKey) {
	outFile, err := os.Create(fileName)
	checkError(err)
	defer outFile.Close()

	err = ioutil.WriteFile(fileName, []byte(key), 0644)
	checkError(err)
}

func saveSSLIV(fileName string, key SSLIV) {
	outFile, err := os.Create(fileName)
	checkError(err)
	defer outFile.Close()

	err = ioutil.WriteFile(fileName, []byte(key), 0644)
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
