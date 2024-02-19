package stripe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/mitchellh/mapstructure"
	"github.com/stripe/stripe-go"
	"github.com/stripe/stripe-go/charge"
	"github.com/stripe/stripe-go/client"
	"github.com/stripe/stripe-go/product"
	"github.com/stripe/stripe-go/sub"
	"io/ioutil"
	"kairos/main/config"
	"kairos/main/model"
	"kairos/main/utils/openssl"
	"net/http"
	"strconv"
)

func SaveAPIKey(userRequestData model.RequestData, intentData map[string]interface{}) (gin.H, error) {

	type UserStripeData struct {
		APIKey    string `json:"apiKey"binding:"required,min=1"`
		KairosKey string `json:"userKey"binding:"required,min=1"`
	}
	u := UserStripeData{}
	err := mapstructure.Decode(userRequestData.Data, &u)
	if err != nil {
		return nil, err
	}


	type InternalStripeDataResponse struct {
		Endpoint string `json:"endpoint" binding:"required,min=1"`
	}

	iResponse := InternalStripeDataResponse{}
	err = mapstructure.Decode(intentData, &iResponse)
	if err != nil {
		return nil, err
	}


	// decrypt key here
	// set is as the API key
	//DecryptedKey := StripeKeyDecrypt(reqData.Value)


	err = VerifyStripeKey(u.APIKey)
	if err != nil {
		return gin.H{
			"errNo": model.ErrorCode.KairosError,
			"msg":   "failure",
			"data":  "invalid API key",
		}, errors.New("could not save stripe key")
	}

	c, err := openssl.Init()
	if err != nil {
		return nil, err
	}

	api, err := c.Encrypt([]byte(u.APIKey))
	if err != nil {
		return nil, err
	}

	reqData := struct {
		APIKey    []byte `json:"apiKey"binding:"required,min=1"`
		KairosKey string `json:"userKey"binding:"required,min=1"`
	}{
		api,
		u.KairosKey,
	}

	response := gin.H{
		"command":     "StripeSaveAPIKey",
		"kairosKey":   userRequestData.KairosKey,
		"intentKey":   userRequestData.IntentKey,
		"requestData": reqData,
	}

	fmt.Println("The endpoint is ", iResponse.Endpoint)
	_, err = SendData(response, iResponse.Endpoint)
	if err != nil {
		return gin.H{
			"errNo": model.ErrorCode.KairosError,
			"msg":   "failure",
			"data":  response,
		}, err
	}

	return gin.H{
		"errNo": model.ErrorCode.SUCCESS,
		"msg":   "success",
		"data":  response,
	}, nil

}

func CreateCharge(userRequestData model.RequestData, intentData map[string]interface{}) (gin.H, error) {

	type ResponseData struct {
		Token string `json:"token" binding:"required,min=1"`
	}
	userDataStripe := ResponseData{}
	mapstructure.Decode(userRequestData.Data, &userDataStripe)

	type InternalStripeDataResponse struct {
		Amount      string `json:"amount" binding:"required, min=1"`
		Name        string `json:"name" binding:"required, min=1"`
		Currency    string `json:"currency" binding:"required, min=1"`
		Description string `json:"description,omitempty"`
		Email       string `json:"email,omitempty"`
		CustomerID  string `json:"customerId,omitempty"`
		ApiKey      string `json:"apiKey"binding:"required,min=1"`
		Endpoint string `json:"endpoint" binding:"required,min=1"`
	}
	iResponse := InternalStripeDataResponse{}
	err := mapstructure.Decode(intentData, &iResponse)
	if err != nil {
		return nil, err
	}

	if userDataStripe.Token != "" {

		stripe.Key = iResponse.ApiKey
		ssl, err := openssl.Init()
		if err != nil {
			panic(err)
		}
		ssl.Decrypt([]byte(iResponse.ApiKey))
		sc := &client.API{}
		sc.Init(stripe.Key, nil)
		amount, err := strconv.Atoi(iResponse.Amount)
		stripeAmount := int64(amount)

		chargeDescription := fmt.Sprintf("%s, %s", iResponse.Email, iResponse.Description)
		chargeParams := &stripe.ChargeParams{
			Amount:      &stripeAmount,
			Currency:    &iResponse.Currency,
			Description: &chargeDescription,
		}
		chargeParams.SetSource(userDataStripe.Token)
		ch, err := charge.New(chargeParams)
		if err != nil {
			return nil, err
		}
		response := gin.H{
			"command":   "CreateSubscription",
			"kairosKey": userRequestData.KairosKey,
			"intentKey": userRequestData.IntentKey,
			"charge":    ch,
		}

		_, err = SendData(response, iResponse.Endpoint)
		if err != nil {
			return gin.H{
				"errNo": model.ErrorCode.KairosError,
				"msg":   "failure",
				"data":  response,
			}, err
		}

		return gin.H{
			"errNo": model.ErrorCode.SUCCESS,
			"msg":   "success",
			"data":  response,
		}, nil

	}

	return nil, errors.New("no stripe token")
}

func CreateSubscription(userRequestData model.RequestData, intentData map[string]interface{}) (gin.H, error) {

	type ResponseData struct {
		Token string `json:"token" binding:"required,min=1"`
		Email string `json:"email" binding:"required,min=1"`
	}
	userDataStripe := ResponseData{}
	mapstructure.Decode(userRequestData.Data, &userDataStripe)

	type InternalStripeDataResponse struct {
		Plan   string `json:"plan" binding:"required, min=1"`
		ApiKey string `json:"apiKey" binding:"required,min=1"`
		Endpoint string `json:"endpoint" binding:"required,min=1"`
	}

	iResponse := InternalStripeDataResponse{}
	err := mapstructure.Decode(intentData, &iResponse)
	if err != nil {
		return nil, err
	}

	if userDataStripe.Token != "" {

		stripe.Key = iResponse.ApiKey

		customer := Customer{}
		cus := customer.CreateCustomer(userDataStripe.Token, userDataStripe.Email)

		ssl, err := openssl.Init()
		if err != nil {
			panic(err)
		}

		ssl.Decrypt([]byte(iResponse.ApiKey))
		sc := &client.API{}
		sc.Init(stripe.Key, nil)

		params := &stripe.SubscriptionParams{
			Customer: stripe.String(cus.ID),
			Items: []*stripe.SubscriptionItemsParams{
				{
					Plan: stripe.String(iResponse.Plan),
				},
			},
		}
		s, err := sub.New(params)
		if err != nil {
			return nil, err
		}
		response := gin.H{
			"command":      "CreateSubscription",
			"kairosKey":    userRequestData.KairosKey,
			"intentKey":    userRequestData.IntentKey,
			"subscription": s,
		}

		_, err = SendData(response, iResponse.Endpoint)
		if err != nil {
			return gin.H{
				"errNo": model.ErrorCode.KairosError,
				"msg":   "failure",
				"data":  response,
			}, err
		}

		return gin.H{
			"errNo": model.ErrorCode.SUCCESS,
			"msg":   "success",
			"data":  response,
		}, nil
	}

	return nil, errors.New("no stripe token")
}

func VerifyStripeKey(key string) error {
	stripe.Key = key
	sc := &client.API{}
	sc.Init(stripe.Key, nil)
	params := &stripe.ProductListParams{}
	params.Filters.AddFilter("limit", "", "3")
	i := product.List(params)
	if i.Err() != nil {
		return errors.New(i.Err().Error())
	}
	return nil
}

func SendData(reqData map[string]interface{}, endpoint string) ([]byte, error) {
	data, err := json.Marshal(reqData)
	req, err := http.NewRequest(config.Post, endpoint, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set(config.ContentType, config.ApplicationJson)
	req.Header.Set(config.Authorization, config.AuthKey)

	requestClient := &http.Client{}
	resp, err := requestClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("status not okay from Kairos")
	}
	return body, nil
}
