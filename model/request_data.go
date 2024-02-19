package model

import "github.com/gin-gonic/gin"

type RequestData struct {
	KairosKey string `json:"userKey" binding:"required,min=1"`
	IntentKey string `json:"commandKey" binding:"required,min=1"`
	Data      gin.H  `json:"data"`
}
