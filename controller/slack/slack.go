package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bluele/slack"
	"github.com/gin-gonic/gin"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"kairos/main/config"
	"kairos/main/model"
	"kairos/main/utils/openssl"
	"net/http"
)

//func NewNotification() {
//	//curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' https://hooks.slack.com/services/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
//	SendData(map[string]interface{} {
//		"text":"Hello golang",
//	}, "https://hooks.slack.com/services/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")
//}

const (
	token       = "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2"
	channelName = "general"
)

func SendMessage(userRequestData model.RequestData, intentData map[string]interface{}) (gin.H, error) {
	type ResponseData struct {
		Text    string `json:"message" binding:"required,min=1"`
		Channel string `json:"channel" binding:"required,min=1"`
	}
	userDataSlack := ResponseData{}
	mapstructure.Decode(userRequestData.Data, &userDataSlack)
	fmt.Println(userDataSlack)

	type InternalSlackResponse struct {
		Text    string `json:"text"binding:"required,min=1"`
		Channel string `json:"channel"binding:"required,min=1"`
		APIKey  string `json:"apiKey"binding:"required,min=1"`
		Endpoint string `json:"endpoint" binding:"required,min=1"`
	}

	iResponse := InternalSlackResponse{}
	err := mapstructure.Decode(intentData, &iResponse)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Listen, the value of userDataSlack is %v\nThe value of userRequestData is %v\n UserIntentData is %v",
		userDataSlack, userRequestData, intentData)
	if iResponse.Text != "" {

		ssl, err := openssl.Init()
		if err != nil {
			panic(err)
		}

		ssl.Decrypt([]byte(iResponse.APIKey))

		err = NewNotification(iResponse.Text, iResponse.Channel, iResponse.APIKey)

		if err != nil {
			return gin.H{
				"errNo": model.ErrorCode.KairosError,
				"msg":   "failure",
			}, err
		}

		response := gin.H{
			"command":   "SlackSendNotification",
			"kairosKey": userRequestData.KairosKey,
			"intentKey": userRequestData.IntentKey,
			"text":      userDataSlack.Text,
			"channel":   userDataSlack.Channel,
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

func NewNotification(notification string, channelName, token string) error {
	api := slack.New(token)
	fmt.Printf("channel name %s, notification %s", channelName, notification)
	err := api.ChatPostMessage(channelName, notification, nil)
	if err != nil {
		panic(err.Error() + " " + token)
		return err
	}
	return nil
}

func NewGroup(groupName string) {
	api := slack.New(token)
	err := api.CreateGroup(groupName)
	if err != nil {
		panic(err)
	}
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

func UsersInfo(userID string) {
	api := slack.New(token)
	user, err := api.UsersInfo(userID)
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Name, user.Profile.Email)
}

func ListUsers() {
	api := slack.New(token)
	users, err := api.UsersList()
	if err != nil {
		panic(err)
	}
	for _, user := range users {
		fmt.Println(user.Id, user.Name)
	}
}

func ListChatChannels() {
	api := slack.New(token)
	channels, err := api.ChannelsList()
	if err != nil {
		panic(err)
	}
	NewNotification("Channel List", "general", "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2")
	for _, channel := range channels {
		fmt.Println(channel.Id, channel.Name)
		NewNotification(channel.Name, "general", "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2")
	}
}

func ChannelInfo() {
	api := slack.New(token)
	channel, err := api.FindChannelByName(channelName)
	if err != nil {
		panic(err)
	}
	msgs, err := api.ChannelsHistoryMessages(&slack.ChannelsHistoryOpt{
		Channel: channel.Id,
	})
	if err != nil {
		panic(err)
	}
	for _, msg := range msgs {
		fmt.Println(msg.UserId, msg.Text)
	}
}
