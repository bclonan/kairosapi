package slack

import "testing"

func TestNewNotification(t *testing.T) {
	NewNotification("hi", "general", "xoxp-388044397831-386300920016-466668367249-aeef50807e1f333200f0e340949542e2")
}

func TestNewGroup(t *testing.T) {
	NewGroup("the hub")
}

func TestListUsers(t *testing.T) {
	ListUsers()
}

func TestUsersInfo(t *testing.T) {
	UsersInfo("UBC8UT20G")
}

func TestListChatChannels(t *testing.T) {
	ListChatChannels()
}

func TestChannelInfo(t *testing.T) {
	ChannelInfo()
}
