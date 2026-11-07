package main

import (
	"testing"
	"time"
)

const obs = "AABBCCDD"

func newTest() (*Merger, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	m := NewMerger([]string{"a", "b"}, time.Minute, "")
	m.Now = func() time.Time { return now }
	return m, &now
}

func pkt(src, iata, raw, ts string) Msg {
	return Msg{src, "meshcore/" + iata + "/" + obs + "/packets", []byte(`{"raw":"` + raw + `","timestamp":"` + ts + `"}`)}
}

func TestSameObserverOnTwoBrokersForwardedOnce(t *testing.T) {
	m, _ := newTest()
	if _, ok := m.Handle(pkt("a", "EDI", "11AA", "t1")); !ok {
		t.Fatal("first copy dropped")
	}
	// different uploader, different envelope, same RF packet
	if _, ok := m.Handle(pkt("b", "EDI", "11aa", "t2")); ok {
		t.Fatal("second copy forwarded")
	}
}

func TestDifferentObserversKept(t *testing.T) {
	m, _ := newTest()
	m.Handle(pkt("a", "EDI", "11AA", "t"))
	other := Msg{"b", "meshcore/EDI/EEFF0011/packets", []byte(`{"raw":"11AA"}`)}
	if _, ok := m.Handle(other); !ok {
		t.Fatal("other observer's copy dropped")
	}
}

func TestWindowExpiry(t *testing.T) {
	m, now := newTest()
	m.Handle(pkt("a", "EDI", "11AA", "t"))
	*now = now.Add(2 * time.Minute)
	if _, ok := m.Handle(pkt("b", "EDI", "11AA", "t")); !ok {
		t.Fatal("repeat after window dropped")
	}
}

func TestRegionPinnedToFirstInput(t *testing.T) {
	m, _ := newTest()
	m.Handle(pkt("b", "GLA", "01", "t"))
	o, _ := m.Handle(pkt("a", "EDI", "02", "t"))
	if o.Topic != "meshcore/EDI/"+obs+"/packets" {
		t.Fatalf("got %s", o.Topic)
	}
	o, _ = m.Handle(pkt("b", "GLA", "03", "t"))
	if o.Topic != "meshcore/EDI/"+obs+"/packets" {
		t.Fatalf("lower priority input moved region: %s", o.Topic)
	}
}

func status(src, st, ts string) Msg {
	return Msg{src, "meshcore/EDI/" + obs + "/status", []byte(`{"status":"` + st + `","timestamp":"` + ts + `"}`)}
}

func TestOfflineSuppressedWhileOnlineElsewhere(t *testing.T) {
	m, _ := newTest()
	if o, ok := m.Handle(status("a", "online", "1")); !ok || !o.Retain {
		t.Fatal("online not forwarded retained")
	}
	m.Handle(status("b", "online", "1"))
	if _, ok := m.Handle(status("a", "offline", "2")); ok {
		t.Fatal("offline forwarded while b online")
	}
	if _, ok := m.Handle(status("b", "offline", "3")); !ok {
		t.Fatal("offline from last input dropped")
	}
}

func TestSourceDownClearsClaims(t *testing.T) {
	m, _ := newTest()
	m.Handle(status("a", "online", "1"))
	m.Handle(status("b", "online", "1"))
	m.SourceDown("a")
	if _, ok := m.Handle(status("b", "offline", "2")); !ok {
		t.Fatal("offline held back by a lost input")
	}
}

func TestTopicRootAndPassthrough(t *testing.T) {
	m, _ := newTest()
	m.TopicRoot = "merged"
	o, ok := m.Handle(Msg{"a", "meshcore/client/ABCD/packets", []byte(`{"x":1}`)})
	if !ok || o.Topic != "merged/client/ABCD/packets" {
		t.Fatalf("got %v %s", ok, o.Topic)
	}
	if _, ok := m.Handle(Msg{"b", "meshcore/client/ABCD/packets", []byte(`{"x":1}`)}); ok {
		t.Fatal("identical client message forwarded twice")
	}
}
