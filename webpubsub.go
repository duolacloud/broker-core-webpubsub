package webpubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/duolacloud/broker-core"
	"github.com/huandu/go-clone"
	wps "github.com/webpubsub/sdk-go/v7"
)

type wpsBroker struct {
	opts        *broker.Options
	wps         *wps.WebPubSub
	mutex       sync.RWMutex
	subscribers map[string][]*wpsSubscriber
}

func NewBroker(opts ...broker.Option) (broker.Broker, error) {
	b := &wpsBroker{
		opts:        broker.NewOptions(),
		subscribers: make(map[string][]*wpsSubscriber),
	}
	if err := b.Init(opts...); err != nil {
		return nil, err
	}
	return b, nil
}

type ContextKey string

const ContextKeyWebPubSub ContextKey = "webpubsub"

func WithWebPubSub(c *wps.WebPubSub) broker.Option {
	return func(o *broker.Options) {
		o.Context = context.WithValue(o.Context, ContextKeyWebPubSub, c)
	}
}

func (b *wpsBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(b.opts)
	}
	client, ok := b.opts.Context.Value(ContextKeyWebPubSub).(*wps.WebPubSub)
	if !ok {
		return errors.New("webpubsub client required")
	}
	b.wps = client
	return nil
}

func (b *wpsBroker) Options() broker.Options {
	return *b.opts
}

func (b *wpsBroker) Address() string {
	return ""
}

func (b *wpsBroker) Connect() error {
	listener := wps.NewListener()
	go func() {
		for {
			select {
			case status := <-listener.Status:
				switch status.Category {
				case wps.WPSDisconnectedCategory:
					// This event happens when radio / connectivity is lost
					fmt.Println("webpubsub disconnected")
				case wps.WPSReconnectedCategory:
					// Happens as part of our regular operation. This event happens when
					// radio / connectivity is lost, then regained.
					fmt.Println("webpubsub reconnected")
				case wps.WPSConnectedCategory:
					// Connect event. You can do stuff like publish, and know you'll get it.
					// Or just use the connected event to confirm you are subscribed for
					// UI / internal notifications, etc
					fmt.Println("webpubsub connected")
				}
			case message := <-listener.Message:
				// handle messages
				fmt.Printf("webpubsub message: %+v\n", message.Message)
				fmt.Printf("webpubsub channel: %+v\n", message.Channel)
				fmt.Printf("webpubsub subscription: %+v\n", message.Subscription)
				fmt.Printf("webpubsub time token: %+v\n", message.Timetoken)
				if err := b.dispatch(message); err != nil {
					fmt.Printf("webpubsub dispatch message error: %+v\n", err)
				}
			}
		}
	}()
	b.wps.AddListener(listener)
	return nil
}

func (b *wpsBroker) Disconnect() error {
	b.wps.Destroy()
	b.wps = nil
	return nil
}

func (b *wpsBroker) Publish(topic string, msg any, opts ...broker.PublishOption) error {
	res, status, err := b.wps.Publish().Channel(topic).Message(msg).Execute()
	fmt.Printf("webpubsub publish: %+v\n", msg)
	fmt.Printf("webpubsub publish response: %+v\n", res)
	fmt.Printf("webpubsub publish status: %+v\n", status)
	return err
}

func (b *wpsBroker) Subscribe(topic string, h broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	subscribers, ok := b.subscribers[topic]
	if !ok {
		subscribers = make([]*wpsSubscriber, 0)
	}

	sub := &wpsSubscriber{
		topic:   topic,
		opts:    broker.NewSubscribeOptions(opts...),
		handler: h,
		broker:  b,
	}
	subscribers = append(subscribers, sub)
	b.subscribers[topic] = subscribers

	if !ok {
		b.wps.Subscribe().Channels([]string{topic}).Execute()
		fmt.Printf("webpubsub subscribe %s\n", topic)
	}
	return sub, nil
}

func (b *wpsBroker) unsubscribe(sub *wpsSubscriber) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	topic := sub.topic
	subscribers, ok := b.subscribers[topic]
	if !ok {
		return nil
	}

	tmp := make([]*wpsSubscriber, 0)
	for _, it := range subscribers {
		if it != sub {
			tmp = append(tmp, it)
		}
	}

	if len(tmp) > 0 {
		b.subscribers[topic] = tmp
		return nil
	}

	delete(b.subscribers, topic)
	b.wps.Unsubscribe().Channels([]string{topic}).Execute()
	fmt.Printf("webpubsub unsubscribe %s\n", topic)
	return nil
}

func (b *wpsBroker) dispatch(m *wps.WPSMessage) error {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	subscribers, ok := b.subscribers[m.Channel]
	if !ok {
		fmt.Printf("webpubsub no subscribers %s\n", m.Channel)
		return nil
	}

	for _, sub := range subscribers {
		e := &event{
			topic: sub.topic,
		}
		if sub.opts.ResultType != nil {
			e.message = clone.Clone(sub.opts.ResultType)
			bs, _ := json.Marshal(m.Message)
			if err := json.Unmarshal(bs, e.message); err != nil {
				e.err = err
				e.message = m.Message
			}
		}
		if e.message == nil {
			e.message = m.Message
		}
		if err := sub.handler(e); err != nil {
			return err
		}
	}
	return nil
}

func (b *wpsBroker) String() string {
	return "webpubsub"
}

type wpsSubscriber struct {
	opts    broker.SubscribeOptions
	topic   string
	handler broker.Handler
	broker  *wpsBroker
}

func (s *wpsSubscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *wpsSubscriber) Topic() string {
	return s.topic
}

func (s *wpsSubscriber) Unsubscribe() error {
	return s.broker.unsubscribe(s)
}

type event struct {
	topic   string
	message any
	err     error
}

func (e *event) Topic() string {
	return e.topic
}

func (e *event) Message() any {
	return e.message
}

func (e *event) Ack() error {
	return nil
}

func (e *event) Error() error {
	return e.err
}
