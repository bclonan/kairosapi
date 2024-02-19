package common

import (
	"github.com/gin-gonic/gin"
	"kairos/main/model"
	"net/http"
)

// SendErrJSON
func SendErrJSON(msg string, args ...interface{}) {
	if len(args) == 0 {
		panic("no *gin.Context")
	}
	var c *gin.Context
	var errNo = model.ErrorCode.ERROR
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
