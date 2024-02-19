package stripe

import (
	"github.com/stripe/stripe-go"
	"github.com/stripe/stripe-go/customer"
)

type Customer struct {
	Token  string
	Amount float64
	Email  string
}

func (c *Customer) CreateCustomer(token string, email ...string) *stripe.Customer {

	var e string
	if len(email) > 0 {
		e = email[0]
	}
	params := &stripe.CustomerParams{
		Email: &e,
	}

	params.SetSource(token)
	cus, err := customer.New(params)
	if err != nil {
		panic(err)
	}

	return cus
}
