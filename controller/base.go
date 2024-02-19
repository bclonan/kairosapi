package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"kairos/main/config"
	"kairos/main/controller/cms"
	"kairos/main/controller/slack"
	"kairos/main/controller/stripe"
	"kairos/main/model"
	"net/http"
	"github.com/fatih/structs"
)

var IntentFunctions = map[string]func(reqData model.RequestData, Intent map[string]interface{}) (gin.H, error){
	"StripeCreateCharge":       stripe.CreateCharge,
	"StripeSaveAPIKey":         stripe.SaveAPIKey,
	"StripeCreateSubscription": stripe.CreateSubscription,
	"SlackSendNotification":    slack.SendMessage,
	"GetPageData":              cms.GetPageContents,
}

type Intent struct {
	Functions map[string]func(ctx *gin.Context)
	Data      map[string]interface{}
}

func NewIntent() *Intent {
	return &Intent{Functions: make(map[string]func(ctx *gin.Context)), Data: make(map[string]interface{})}
}

func GetIntent(c *gin.Context) {

	SendErrJson := func(msg string, args ...interface{}) {
		if len(args) == 0 {
			panic("no *gin.Context")
		}
		var c *gin.Context
		var errNo = 401
		if len(args) == 1 {
			theCtx, ok := args[0].(*gin.Context)
			if !ok {
				panic("no *gin.Context")
			}
			c = theCtx
		} else if len(args) == 2 {
			theErrNo, ok := args[0].(int)
			if !ok {
				panic("errNo incorrect")
			}
			errNo = theErrNo
			theCtx, ok := args[1].(*gin.Context)
			if !ok {
				panic("no *gin.Context")
			}
			c = theCtx
		}
		c.JSON(http.StatusOK, gin.H{
			"errNo": errNo,
			"msg":   msg,
			"data":  gin.H{},
		})
		c.Abort()
	}

	var reqData model.RequestData
	if err := c.ShouldBindJSON(&reqData); err != nil {
		SendErrJson("Invalid Argument", c)
		return
	}

	if reqData.KairosKey == "" {
		SendErrJson("No Kairos PageData Specified", c, 401)
		return
	}

	data, err := json.Marshal(struct {
		KairosKey string `json:"kairosKey" binding:"required,min=1"`
		IntentKey string `json:"intentKey" binding:"required,min=1"`
	}{reqData.KairosKey,
		reqData.IntentKey,
	})

	if err != nil {
		SendErrJson("Invalid Request Data")
	}

	const GetIntentURL = "http://localhost:8075/test/get-intent"
	req, err := http.NewRequest(config.Post, GetIntentURL, bytes.NewBuffer(data))
	if err != nil {
		SendErrJson(err.Error(), c, 401)
	}

	req.Header.Set(config.ContentType, config.ApplicationJson)
	req.Header.Set(config.Authorization, config.AuthKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		SendErrJson(err.Error(), c, resp.StatusCode)
	}
	body, _ := ioutil.ReadAll(resp.Body)

	intentResponse := model.IntentResponse{}
	json.Unmarshal(body, &intentResponse)

	if resp.StatusCode != 200 {
		SendErrJson("invalid response", c, resp.StatusCode)
	}


	if reqData.IntentKey == "" {
		SendErrJson("No IntentKey Specified", c, 401)
		return
	}

	type IntentData struct {
		IntentFunction string
		IntentData     map[string]interface{}
	}
	m := make([]IntentData, len(intentResponse.Functions))
	for key, intention := range intentResponse.Functions {
		//fmt.Println(key-1, intention)
		if data, ok := intentResponse.Data[intention].(map[string]interface{}); ok {
			m[key-1] = IntentData{IntentFunction: intention, IntentData: data}
		}
	}
	// this is where the magic happens -> traverse and systematically executes one function at a time
	responses := make([]gin.H, 0)
	for i, intention := range m {
		funcName(structs.Map(intention), reqData)
		resp, e := IntentFunctions[intention.IntentFunction](reqData, intention.IntentData)
		if e != nil {
			c.JSON(401, map[string]interface{}{
				"error":          "one or more intention functions could not be executed",
				"errorNo":        e,
				"stepFailed":     i + 1,
				"stepsRemaining": len(m) - (i + 1),
				"intent":         intention,
			})
			return
		}
		responses = append(responses, resp)
	}
	c.JSON(200, responses)

}

func funcName(intention map[string]interface{}, reqData model.RequestData) {
	fmt.Printf("Executing function %s\n", intention)
	fmt.Printf("Passing in reqData: %v\n", reqData)
}
