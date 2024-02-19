package openssl

import "io/ioutil"

func Init() (*Crypter, error) {
	iv, err := ioutil.ReadFile("utils/openssl/keys/iv.ssl")
	if err != nil {
		return nil, err
	}

	key, err := ioutil.ReadFile("utils/openssl/keys/key.ssl")
	if err != nil {
		return nil, err
	}

	c, err := NewCrypter(key, iv)
	if err != nil {
		return nil, err
	}

	return c, err
}
