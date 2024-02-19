package router

import (
	"kairos/main/config"
	"kairos/main/controller"
	"kairos/main/controller/status"
	"kairos/main/model"

	"github.com/gin-gonic/gin"
)

func Route(router *gin.Engine) {
	api := router.Group(config.APIDefaultRoute)
	{
		api.GET("/is-up", status.IsUp)
		api.POST("", controller.GetIntent)
	}

	test := router.Group(config.APITestRoute)
	{
		bools := test.Group("/success")
		{
			bools.POST("/true", func(c *gin.Context) {
				c.JSON(200, "true")
			})
			bools.POST("/false", func(c *gin.Context) {
				c.JSON(400, "false")
			})
		}
		stripe := test.Group("/stripe")
		{
			stripe.POST("/createcharge", func(c *gin.Context) {

				response := model.IntentResponse{
					Functions: map[int]string{
						1: "StripeCreateCharge",
					},
					Data: map[string]interface{}{
						"StripeCreateCharge": map[string]string{
							"Email":       "clonanxyz@gmail.com",
							"Amount":      "1999",
							"Title":       "Some Title For The Market",
							"Name":        "Zachary J ",
							"Currency":    "usd",
							"Description": "Something",
							"PageData":    "sk_test_kqcgtNglf8UIlDtPs7ziTKzO",
						}, "Slack": map[string]string{
							"API":          "slack_ck4chs7",
							"Notification": "Hello world!",
						},
					},
				}

				c.JSON(200, response)
			})

			stripe.POST("/addkey", func(c *gin.Context) {
				response := model.IntentResponse{
					Functions: map[int]string{
						1: "StripeSaveAPIKey",
					},
					Data: map[string]interface{}{
						"StripeSaveAPIKey": map[string]string{},
						"Endpoint":         "http://localhost:8075/test/success/true",
					},
				}

				c.JSON(200, response)
			})

			test.POST("/get-intent", func(c *gin.Context) {
				type ReqData struct {
					KairosKey string `json:"kairosKey" binding:"required,min=1"`
					IntentKey string `json:"intentKey" binding:"required,min=1"`
				}
				reqData := ReqData{}
				if err := c.ShouldBindJSON(&reqData); err != nil {
					return
				}

				if reqData.IntentKey == "tok_stripe_save_key" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "StripeSaveAPIKey",
						},
						Data: map[string]interface{}{
							"StripeSaveAPIKey": map[string]string{},
							"Endpoint":         "http://localhost:8075/test/success/true",
						},
					}

					c.JSON(200, response)
					return
				}
				if reqData.IntentKey == "tok_stripe_subscription" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "StripeCreateSubscription",
						},
						Data: map[string]interface{}{
							"StripeCreateSubscription": map[string]string{
								"plan":     "plan_123",
								"apiKey":   "sk_test_kqcgtNglf8UIlDtPs7ziTKzO",
								"Endpoint": "http://localhost:8075/test/success/true",
							},
						},
					}

					c.JSON(200, response)
					return
				}

				if reqData.IntentKey == "tok_stripe_charge" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "StripeCreateCharge",
						},
						Data: map[string]interface{}{
							"StripeCreateCharge": map[string]string{
								"Email":       "zjc@gmail.com",
								"Amount":      "1999",
								"Title":       "Zachary Jozef A Shopify",
								"Name":        "Zachary Jozef A",
								"Currency":    "usd",
								"Description": "Something",
								"Endpoint":    "http://localhost:8075/test/success/true",
								"apiKey":      "sk_test_kqcgtNglf8UIlDtPs7ziTKzO",
							}, "Slack": map[string]string{
								"API":          "slack_ck4chs7",
								"Notification": "Hello world!",
								"Endpoint":     "http://localhost:8075/test/success/true",
							},
						},
					}

					c.JSON(200, response)
				}
				if reqData.IntentKey == "tok_slack_webhook" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "SlackSendNotification",
						},
						Data: map[string]interface{}{
							"SlackSendNotification": map[string]string{
								"text":     "new purchase",
								"channel":  "general",
								"apiKey":   "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2",
								"Endpoint": "http://localhost:8075/test/success/true",
							},
						},
					}

					c.JSON(200, response)
				}
				if reqData.IntentKey == "tok_combo" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "SlackSendNotification",
							2: "StripeCreateCharge",
							3: "StripeSaveAPIKey",
							4: "StripeCreateCharge",
						},
						Data: map[string]interface{}{
							"SlackSendNotification": map[string]string{
								"text":     "new purchase",
								"channel":  "general",
								"apiKey":   "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2",
								"Endpoint": "http://localhost:8075/test/success/true",
							},
							"StripeCreateCharge": map[string]string{
								"Email":       "Zachary JozefA@gmail.com",
								"Amount":      "1999",
								"Title":       "Zachary Jozef A's Sovrin Mind",
								"Name":        "Zachary Jozef A",
								"Currency":    "usd",
								"Description": "Something",
								"apiKey":      "sk_test_kqcgtNglf8UIlDtPs7ziTKzO",
								"Endpoint":    "http://localhost:8075/test/success/true",
							},
							"StripeSaveAPIKey": map[string]string{
								"Endpoint": "http://localhost:8075/test/success/true",
							},
						},
					}

					c.JSON(200, response)
				}
				if reqData.IntentKey == "tok_cms" {
					response := model.IntentResponse{
						Functions: map[int]string{
							1: "GetPageData",
						},
						Data: map[string]interface{}{
							"GetPageData": map[string]interface{}{
								"pageData": map[string]interface{}{
									"h1": "Hello",
									"h2": "World",
									"p": map[string]interface{}{
										"class":   "Zachary Jozef",
										"id":      123,
										"v-model": "func(joe){}",
									},
								},
							},
						},
					}

					c.JSON(200, response)
				}
			})
		}

	}
}
