package cms

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/mitchellh/mapstructure"
	"kairos/main/model"
)

func GetPageContents(userRequestData model.RequestData, intentData map[string]interface{}) (gin.H, error) {

	type InternalCMSData struct {
		PageData map[string]interface{} `json:"pageData"binding:"required,min=1"`
	}

	responseFromInternalServerRegardingTheirChargeInformation := InternalCMSData{}
	err := mapstructure.Decode(intentData, &responseFromInternalServerRegardingTheirChargeInformation)
	if err != nil {
		return nil, err
	}

	if responseFromInternalServerRegardingTheirChargeInformation.PageData != nil {

		response := gin.H{
			"command":   "GetPageData",
			"kairosKey": userRequestData.KairosKey,
			"intentKey": userRequestData.IntentKey,
			"data":      responseFromInternalServerRegardingTheirChargeInformation.PageData,
		}

		return gin.H{
			"errNo": model.ErrorCode.SUCCESS,
			"msg":   "success",
			"data":  response,
		}, nil
	}

	return gin.H{
		"errNo": model.ErrorCode.KairosError,
		"msg":   "failure",
	}, errors.New("no data received")
}
