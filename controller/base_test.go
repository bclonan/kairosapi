package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"kairos/main/model"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestBase(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request, _ = http.NewRequest("POST", "/", bytes.NewBufferString(`{"intentFunctions":{"1":"Stripe","2":"Slack"},"intentData":{"Stripe":{"Amount":"1999","Email":"Zachary JozefA@gmail.com","Title":"Zachary Jozef A's Sovrin Mind"},"Slack":{"API":"slack_ck4chs7","Notification":"Hello world!"}}}`))
	c.Request.Header.Add("Content-Type", gin.MIMEJSON)

	obj := model.IntentResponse{}
	assert.NoError(t, c.ShouldBind(&obj))
	assert.Equal(t, "Stripe", obj.Functions[1])
	type IntentData struct {
		IntentFunction string
		IntentData     map[string]interface{}
	}

	m := make([]IntentData, len(obj.Functions))

	for key, intention := range obj.Functions {
		fmt.Println(key, intention)
		if data, ok := obj.Data[intention].(map[string]interface{}); ok {
			m[key-1] = IntentData{IntentFunction: intention, IntentData: data}
			for dKey, datum := range data {
				fmt.Println(dKey, datum)
			}
		}
	}

	for t, i := range m {
		fmt.Println(t, i)
	}

	if data, ok := obj.Data["Stripe"].(map[string]interface{}); ok {
		assert.Equal(t, "1999", data["Amount"])
	}
	assert.Empty(t, c.Errors)
}

func TestSliceAppropriation(t *testing.T) {
}

func TestBaseData(t *testing.T) {
	type ReqData struct {
		KairosKey string `json:"userKey" binding:"required,min=1"`
		IntentKey string `json:"commandKey" binding:"required,min=1"`
		Endpoint  string `json:"endpoint" binding:"required,min=1"`
		Data      gin.H  `json:"data"`
	}
	KairosKey := "r_test_jd8jkl1"
	IntentKey := "fz2v47ll2wz"
	Endpoint := ""
	Data := gin.H{
		"amount": 1999,
	}
	req := ReqData{KairosKey: KairosKey, IntentKey: IntentKey, Endpoint: Endpoint, Data: Data}
	marshalled, err := json.Marshal(req)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(marshalled))
}
