package model

type IntentResponse struct {
	Functions map[int]string         `json:"intentFunctions"`
	Data      map[string]interface{} `json:"intentData"`
}
