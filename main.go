package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Broker struct {
	Name     string   `json:"name"`
	URL      string   `json:"url"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	ClientID string   `json:"clientId"`
	Insecure bool     `json:"insecure"`
	Topics   []string `json:"topics"`
}

type Output struct {
	Broker
	QoS       byte   `json:"qos"`
	TopicRoot string `json:"topicRoot"`
}

type Config struct {
	Inputs        []Broker `json:"inputs"`
	Output        Output   `json:"output"`
	DedupSeconds  int      `json:"dedupSeconds"`
	StatsInterval int      `json:"statsSeconds"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{DedupSeconds: 60, StatsInterval: 300}
	if err := json.Unmarshal([]byte(os.ExpandEnv(string(b))), cfg); err != nil {
		return nil, err
	}
	if len(cfg.Inputs) == 0 || cfg.Output.URL == "" {
		return nil, errors.New("need at least one input and an output url")
	}
	names := map[string]bool{}
	for i := range cfg.Inputs {
		in := &cfg.Inputs[i]
		if in.Name == "" {
			in.Name = in.URL
		}
		if names[in.Name] {
			return nil, fmt.Errorf("duplicate input name %q", in.Name)
		}
		names[in.Name] = true
		if len(in.Topics) == 0 {
			in.Topics = []string{"meshcore/#"}
		}
		if in.URL == cfg.Output.URL && cfg.Output.TopicRoot == "" {
			return nil, fmt.Errorf("input %q is the output broker; set output.topicRoot to avoid a loop", in.Name)
		}
	}
	if cfg.Output.Name == "" {
		cfg.Output.Name = "output"
	}
	return cfg, nil
}

var unsafeID = regexp.MustCompile(`[^A-Za-z0-9-]+`)

func options(b Broker) *mqtt.ClientOptions {
	id := b.ClientID
	if id == "" {
		r := make([]byte, 4)
		rand.Read(r)
		id = "mcmerge-" + strings.Trim(unsafeID.ReplaceAllString(b.Name, "-"), "-") + "-" + hex.EncodeToString(r)
	}
	o := mqtt.NewClientOptions().
		AddBroker(b.URL).
		SetClientID(id).
		SetUsername(b.Username).
		SetPassword(b.Password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(time.Minute).
		SetOrderMatters(false)
	if b.Insecure {
		o.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	return o
}

func run(cfg *Config, stop <-chan os.Signal) {
	names := make([]string, len(cfg.Inputs))
	for i, in := range cfg.Inputs {
		names[i] = in.Name
	}
	m := NewMerger(names, time.Duration(cfg.DedupSeconds)*time.Second, cfg.Output.TopicRoot)

	oo := options(cfg.Output.Broker).
		SetOnConnectHandler(func(mqtt.Client) { log.Printf("[%s] connected", cfg.Output.Name) }).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { log.Printf("[%s] lost: %v", cfg.Output.Name, err) })
	out := mqtt.NewClient(oo)
	out.Connect()

	var dropped atomic.Uint64
	var clients []mqtt.Client
	for _, in := range cfg.Inputs {
		in := in
		handler := func(_ mqtt.Client, msg mqtt.Message) {
			o, ok := m.Handle(Msg{Source: in.Name, Topic: msg.Topic(), Payload: msg.Payload()})
			if !ok {
				return
			}
			if !out.IsConnectionOpen() {
				dropped.Add(1)
				return
			}
			out.Publish(o.Topic, cfg.Output.QoS, o.Retain, o.Payload)
		}
		io := options(in).
			SetOnConnectHandler(func(c mqtt.Client) {
				log.Printf("[%s] connected", in.Name)
				filters := map[string]byte{}
				for _, t := range in.Topics {
					filters[t] = 0
				}
				if t := c.SubscribeMultiple(filters, handler); t.Wait() && t.Error() != nil {
					log.Printf("[%s] subscribe: %v", in.Name, t.Error())
				}
			}).
			SetConnectionLostHandler(func(_ mqtt.Client, err error) {
				log.Printf("[%s] lost: %v", in.Name, err)
				m.SourceDown(in.Name)
			})
		c := mqtt.NewClient(io)
		c.Connect()
		clients = append(clients, c)
	}

	tick := time.NewTicker(time.Duration(max(cfg.StatsInterval, 1)) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			stats := m.Stats()
			for _, n := range names {
				s := stats[n]
				log.Printf("[%s] in=%d out=%d dup=%d suppressed=%d", n, s.In, s.Out, s.Dup, s.Suppressed)
			}
			if d := dropped.Load(); d > 0 {
				log.Printf("[%s] dropped %d while disconnected", cfg.Output.Name, d)
			}
		case <-stop:
			for _, c := range clients {
				c.Disconnect(250)
			}
			out.Disconnect(1000)
			return
		}
	}
}

func main() {
	path := flag.String("config", "config.json", "config file")
	flag.Parse()
	cfg, err := loadConfig(*path)
	if err != nil {
		log.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	run(cfg, stop)
}
