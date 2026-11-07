package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func broker(t *testing.T, id string) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	s := mochi.New(&mochi.Options{InlineClient: true})
	s.AddHook(new(auth.AllowHook), nil)
	if err := s.AddListener(listeners.NewTCP(listeners.Config{ID: id, Address: addr})); err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	t.Cleanup(func() { s.Close() })
	return "tcp://" + addr
}

func client(t *testing.T, url string) mqtt.Client {
	c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(url).SetClientID(fmt.Sprint("t", time.Now().UnixNano())))
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

func TestEndToEnd(t *testing.T) {
	a, b, out := broker(t, "a"), broker(t, "b"), broker(t, "out")
	cfg := &Config{
		Inputs:        []Broker{{Name: "a", URL: a, Topics: []string{"meshcore/#"}}, {Name: "b", URL: b, Topics: []string{"meshcore/#"}}},
		Output:        Output{Broker: Broker{Name: "out", URL: out}},
		DedupSeconds:  60,
		StatsInterval: 60,
	}
	stop := make(chan os.Signal)
	done := make(chan struct{})
	go func() { run(cfg, stop); close(done) }()
	defer func() { close(stop); <-done }()

	var mu sync.Mutex
	var got []string
	sub := client(t, out)
	sub.Subscribe("meshcore/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		mu.Lock()
		got = append(got, m.Topic())
		mu.Unlock()
	}).Wait()

	pa, pb := client(t, a), client(t, b)
	time.Sleep(500 * time.Millisecond)

	pkt := func(c mqtt.Client, observer, ts string) {
		c.Publish("meshcore/EDI/"+observer+"/packets", 0, false, `{"raw":"1500AABB","timestamp":"`+ts+`"}`).Wait()
	}
	pkt(pa, "OBS1", "1")
	pkt(pb, "OBS1", "2") // same observer, second broker
	pkt(pb, "OBS2", "1") // second observer heard the same packet

	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("want 2 messages, got %v", got)
	}
}
