package model

type errorCode struct {
	SUCCESS      int
	ERROR        int
	NotFound     int
	LoginError   int
	LoginTimeout int
	InActive     int
	StripeError  int
	KairosError  int
}

// ErrorCode 错误码
var ErrorCode = errorCode{
	SUCCESS:      0,
	ERROR:        1,
	NotFound:     404,
	LoginError:   1000,
	LoginTimeout: 1001, //Login timeout
	InActive:     1002,
	StripeError:  401,
	KairosError:  402,
}
