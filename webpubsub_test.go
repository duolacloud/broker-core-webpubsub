package webpubsub

import (
	"testing"
	"time"

	"github.com/duolacloud/broker-core"
	"github.com/stretchr/testify/assert"
	webpubsub "github.com/webpubsub/sdk-go/v7"
)

type User struct {
	Name string `json:"name"`
	Age  int32  `json:"age"`
}

func TestBroker(t *testing.T) {
	wpsConfig := webpubsub.NewConfig(webpubsub.GenerateUUID())
	wpsConfig.SubscribeKey = "sub-c-9da4b596-1075-4736-94b7-d76c13095edd"
	wpsConfig.PublishKey = "pub-c-7621f70b-f767-49a5-a625-d615a25bc1f0"
	wpsConfig.Secure = true
	wpsClient := webpubsub.NewWebPubSub(wpsConfig)

	b, err := NewBroker(WithWebPubSub(wpsClient))
	assert.Nil(t, err)
	err = b.Connect()
	assert.Nil(t, err)

	pubMsg := &User{
		Name: "jack",
		Age:  21,
	}

	doneSubscribe := make(chan bool)
	sub, err := b.Subscribe("test-broker", func(e broker.Event) error {
		t.Logf("subscribed: %+v", e.Message())
		u := e.Message().(*User)
		assert.Equal(t, pubMsg.Age, u.Age)
		assert.Equal(t, pubMsg.Name, u.Name)
		doneSubscribe <- true
		return nil
	}, broker.ResultType(&User{}))
	assert.Nil(t, err)

	go func() {
		time.Sleep(3 * time.Second)
		err := b.Publish("test-broker", pubMsg)
		assert.Nil(t, err)
	}()

	<-doneSubscribe
	err = sub.Unsubscribe()
	assert.Nil(t, err)
}
