package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ADT_A01 = []string{
		"MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||202401201630||ADT^A01|||2.5",
		"PID|1||478237423||ELIZABETH HOLMES||19840203|F",
		"NK1|1|SUNNY BALWANI|PARTNER",
	}

	ORU_R01 = []string{
		"MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||202401201800||ORU^R01|||2.5",
		"PID|1||478237423",
		"OBR|1||||||20240120224300",
		"OBX|1|SN|CREATININE||103.4",
	}

	ADT_A03 = []string{
		"MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||202401221000||ADT^A03|||2.5",
		"PID|1||478237423",
	}

	ACK = []string{
		"MSH|^~\\&|||||20240129093837||ACK|||2.5",
		"MSA|AA",
	}
)

func TestFromMLLP(t *testing.T) {
	var b bytes.Buffer
	for _, message := range [][]string{ADT_A01, ORU_R01, ADT_A03} {
		b.WriteByte(MLLP_START_OF_BLOCK)
		for _, segment := range message {
			b.Write([]byte(segment))
			b.WriteByte('\r')
		}
		b.WriteByte(MLLP_END_OF_BLOCK)
		b.WriteByte(MLLP_CARRIAGE_RETURN)
	}
	messages, remaining, err := fromMLLP(b.Bytes())
	if err != nil {
		t.Fatalf("Expected no error, found %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("Expected 3 messages, found %d", len(messages))
	}
	if len(remaining) != 0 {
		t.Errorf("Expected no remaining bytes, found %d", len(remaining))
	}
}

func TestParseADTA01Message(t *testing.T) {
	joined := RawMessage(strings.Join(ADT_A01, "\r") + "\r")
	message, err := parseMessage(joined)
	if err != nil {
		t.Fatalf("Expected no error, found: %v", err)
	}
	if message.MRN != 478237423 {
		t.Errorf("Unexpected MRN")
	}
	if !bytes.Equal(message.RawMessage, joined) {
		t.Errorf("Unexpected RawMessage")
	}
}

func TestParseORUR01Message(t *testing.T) {
	joined := RawMessage(strings.Join(ORU_R01, "\r") + "\r")
	message, err := parseMessage(joined)
	if err != nil {
		t.Fatalf("Expected no error, found: %v", err)
	}
	if !message.HasObservation {
		t.Errorf("Expected HasObservation")
	}
	if !message.ObservationCreatinine {
		t.Errorf("Expected ObservationCreatinine")
	}
	expected := time.Date(2024, 1, 20, 22, 43, 0, 0, time.UTC)
	if message.ObservationTime != SimulatedTime(expected) {
		t.Errorf("Expected ObservationTime %s, found %s", expected.Format(time.UnixDate), time.Time(message.ObservationTime).Format(time.UnixDate))
	}
}

type ShortConnection struct {
	B bytes.Buffer
}

func (s *ShortConnection) Read(b []byte) (n int, err error) {
	if len(b) > 13 { // Split data into smaller, prime length, blocks
		b = b[0:13]
	}
	return s.B.Read(b)
}

func (s *ShortConnection) Write(b []byte) (n int, err error) {
	return s.B.Write(b)
}

func (s ShortConnection) Close() error                       { return nil }
func (s ShortConnection) LocalAddr() net.Addr                { return nil }
func (s ShortConnection) RemoteAddr() net.Addr               { return nil }
func (s ShortConnection) SetDeadline(t time.Time) error      { return nil }
func (s ShortConnection) SetReadDeadline(t time.Time) error  { return nil }
func (s ShortConnection) SetWriteDeadline(t time.Time) error { return nil }

func TestReadPartialMessages(t *testing.T) {
	message := RawMessage(strings.Join(ORU_R01, "\r") + "\r")
	sc := &ShortConnection{}
	var buffer []byte
	for i := 0; i < 100; i++ {
		buffer = toMLLP(message, buffer[0:])
		sc.B.Write(buffer)
	}
	c := MLLPConnection{C: sc}
	for i := 0; i < 100; i++ {
		r, err := c.Read()
		if err != nil {
			t.Errorf("Expected no error for message %d, found %s", i, err)
			if bytes.Equal(r, message) {
				t.Errorf("Unexpected message contents")
			}
		}
	}
}

func TestShutdownHospital(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := HospitalOptions{
		Name:      "Riverside",
		MLLPAddr:  "0.0.0.0:11932",
		PagerAddr: "0.0.0.0:11933",
		Context:   ctx,
		Log:       log.New(ioutil.Discard, "", 0),
	}
	h, err := NewHospital(&options)
	if err != nil {
		t.Fatalf("Expected no error creating hospital, found: %s", err)
	}

	var g errgroup.Group
	g.Go(func() error {
		return h.Run()
	})
	cancel()
	if err := g.Wait(); err != nil {
		t.Errorf("Expected no error, found: %s", err)
	}
}

func TestPageResponses(t *testing.T) {
	cases := []struct {
		Body       string
		StatusCode int
	}{
		{"1234", http.StatusOK},
		{"NHS1234", http.StatusBadRequest},
		{"1234,202401221000", http.StatusOK},
		{"1234,2024/01/22 10:00", http.StatusBadRequest},
		{"NHS1234,202401221000", http.StatusBadRequest},
		{"1234,202401221000,unused", http.StatusBadRequest},
	}

	for _, c := range cases {
		options := HospitalOptions{
			Name: "Riverside",
		}
		f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
			request := httptest.NewRequest("POST", pager, strings.NewReader(c.Body))
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			result := response.Result()
			if result.StatusCode != c.StatusCode {
				t.Errorf("Expected status %d, found %d for body %q", c.StatusCode, result.StatusCode, c.Body)
			}
		}
		withHospital(options, t, f)
	}
}

func withHospital(options HospitalOptions, t *testing.T, f func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T)) {
	ctx, cancel := context.WithCancel(context.Background())
	options.Context = ctx
	options.Name = "South Riverside"
	options.MLLPAddr = "0.0.0.0:11932"
	options.PagerAddr = "0.0.0.0:11933"
	options.Log = log.New(ioutil.Discard, "", 0)

	h, err := NewHospital(&options)
	if err != nil {
		t.Fatalf("Expected no error creating hospital, found: %s", err)
	}

	var g errgroup.Group
	g.Go(func() error {
		return h.Run()
	})
	defer func() {
		cancel()
		if err := g.Wait(); err != nil {
			t.Fatalf("failed to Run() Hospital: %s", err)
		}
	}()

	connect := func() *MLLPConnection {
		attempts := 0
		var c net.Conn
		for {
			var err error
			if c, err = net.Dial("tcp", options.MLLPAddr); err != nil {
				attempts++
				if attempts == 10 {
					t.Fatal("MLLP server was never ready")
				}
				continue
			}
			break
		}
		return &MLLPConnection{C: c}
	}

	pager := fmt.Sprintf("http://%s/page", options.PagerAddr)
	f(connect, pager, h, t)
}

func TestAckOneMessage(t *testing.T) {
	mrn := MRN(71880957)
	rt := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt,
				RawMessage:            []byte("MSH|..."),
			},
		},
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		c := connect()
		_, err := c.Read()
		if err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		}

		ack := RawMessage(strings.Join(ACK, "\r") + "\r")
		if err := c.Write(ack); err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		}
	}
	withHospital(options, t, f)
}

func TestPageTruePositiveWithNoExplicitTime(t *testing.T) {
	mrn := MRN(71880957)
	rt := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt,
				RawMessage:            []byte("MSH|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt,
		},
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		c := connect()
		_, err := c.Read()
		if err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		}
		body := strings.NewReader(fmt.Sprintf("%d", mrn))
		if r, err := http.Post(pager, "text/plain", body); err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		} else if r.StatusCode != http.StatusOK {
			t.Fatalf("Expected status %s, found %s", http.StatusText(http.StatusOK), http.StatusText(r.StatusCode))
		}

		ack := RawMessage(strings.Join(ACK, "\r") + "\r")
		if err := c.Write(ack); err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		}

		metrics := h.Metrics()
		if metrics.Pages.TruePositives != 1 {
			t.Errorf("Expected one true positive, found %d", metrics.Pages.TruePositives)
		}
	}
	withHospital(options, t, f)
}

func TestPageFalseNegativeWithNoExplicitTime(t *testing.T) {
	mrn := MRN(71880957)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 14, 20, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt1,
		},
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		c := connect()
		for i := 0; i < 2; i++ {
			_, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
			ack := RawMessage(strings.Join(ACK, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
		}

		body := strings.NewReader(fmt.Sprintf("%d", mrn))
		if r, err := http.Post(pager, "text/plain", body); err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		} else if r.StatusCode != http.StatusOK {
			t.Fatalf("Expected status %s, found %s", http.StatusText(http.StatusOK), http.StatusText(r.StatusCode))
		}

		metrics := h.Metrics()
		if metrics.Pages.FalseNegatives != 1 {
			t.Errorf("Expected one false negative, found %d", metrics.Pages.TruePositives)
		}
	}
	withHospital(options, t, f)
}

func TestComputeMetricsForNHSModel(t *testing.T) {
	mrn1 := MRN(71880957)
	mrn2 := MRN(34902420)
	mrn3 := MRN(49320242)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 14, 20, 0, 0, time.UTC))
	rt3 := SimulatedTime(time.Date(2024, 02, 11, 14, 25, 0, 0, time.UTC))
	rt4 := SimulatedTime(time.Date(2024, 02, 11, 14, 30, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn1,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|..."),
			},
			{
				MRN:                   mrn2,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|..."),
			},
			{
				MRN:                   mrn3,
				Time:                  rt3,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt3,
				RawMessage:            []byte("MSH|..."),
			},
			// Sentinel message to ensure the hospital has updated
			// metrics before we query it
			{
				MRN:                   mrn3,
				Time:                  rt4,
				HasObservation:        false,
				ObservationCreatinine: false,
				ObservationTime:       rt4,
				RawMessage:            []byte("MSH|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn1: rt1,
			mrn3: rt3,
		},
		NHSDetectedAKIs: map[MRN]SimulatedTime{
			mrn2: rt2,
			mrn3: rt3,
		},
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		c := connect()
		for _ = range options.Messages {
			_, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
			ack := RawMessage(strings.Join(ACK, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
		}

		metrics := h.Metrics()
		if metrics.NHS.TruePositives != 1 {
			t.Errorf("Expected one true positive, found %d", metrics.Pages.TruePositives)
		}
		if metrics.NHS.FalsePositives != 1 {
			t.Errorf("Expected one false positive, found %d", metrics.Pages.FalsePositives)
		}
		if metrics.NHS.FalseNegatives != 1 {
			t.Errorf("Expected one false negative, found %d", metrics.Pages.FalseNegatives)
		}
	}
	withHospital(options, t, f)
}

func TestPageForPreviousResultWithExplicitTime(t *testing.T) {
	mrn := MRN(71880957)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 14, 20, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt1,
		},
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		c := connect()
		for i := 0; i < 2; i++ {
			_, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
			ack := RawMessage(strings.Join(ACK, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
		}

		body := strings.NewReader(fmt.Sprintf("%d,20240211141500", mrn))
		if r, err := http.Post(pager, "text/plain", body); err != nil {
			t.Fatalf("Expected no error, found: %s", err)
		} else if r.StatusCode != http.StatusOK {
			t.Fatalf("Expected status %s, found %s", http.StatusText(http.StatusOK), http.StatusText(r.StatusCode))
		}

		metrics := h.Metrics()
		if metrics.Pages.TruePositives != 1 {
			t.Errorf("Expected one true positive, found %d", metrics.Pages.TruePositives)
		}
		if metrics.Pages.FalseNegatives != 0 {
			t.Errorf("Expected no false negatives, found %d", metrics.Pages.FalseNegatives)
		}
	}
	withHospital(options, t, f)
}

func TestPersistMessageSendsBetweenRestarts(t *testing.T) {
	directory := t.TempDir()

	mrn := MRN(71880957)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 14, 20, 0, 0, time.UTC))
	rt3 := SimulatedTime(time.Date(2024, 02, 11, 14, 25, 0, 0, time.UTC))

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|...|202402111415|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|...|202402111420|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt3,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt3,
				RawMessage:            []byte("MSH|...|202402111425|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt1,
		},
		EventLogFilename: directory + "/riverside.log",
		ReplayInRealTime: false,
	}

	for expected := 0; expected < len(options.Messages); expected++ {
		f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
			c := connect()
			raw, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s on message %d", err, expected)
			}
			ack := RawMessage(strings.Join([]string{ACK[0], ACK[1] + fmt.Sprintf("|%d", expected)}, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}

			if !bytes.Equal(raw, options.Messages[expected].RawMessage) {
				t.Errorf("Expected message %d, found %s", expected, string(raw))
			}
		}
		withHospital(options, t, f)
	}
}

func TestPersistRelativeTimeBetweenRestarts(t *testing.T) {
	directory := t.TempDir()

	mrn := MRN(71880957)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 16, 00, 0, 0, time.UTC))
	rt3 := SimulatedTime(time.Date(2024, 02, 11, 17, 00, 0, 0, time.UTC))

	var now RealTime

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|...|202402111415|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|...|202402111420|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt3,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt3,
				RawMessage:            []byte("MSH|...|202402111425|..."),
			},
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt1,
		},
		EventLogFilename: directory + "/riverside.log",
		Now: func() RealTime {
			return now
		},
		ReplayInRealTime: true,
	}

	times := []RealTime{
		RealTime(time.Time(rt1).AddDate(0, -1, 0)),
		RealTime(time.Time(rt2).AddDate(0, -1, 0)),
		RealTime(time.Time(rt3).AddDate(0, -1, 0)),
	}

	simulatedStarts := []SimulatedTime{
		rt1.Add(5 * time.Second),
		rt2.Add(5 * time.Second),
		rt3.Add(5 * time.Second),
	}

	for i := range times {
		now = times[i]
		f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
			c := connect()
			_, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
			metrics := h.Metrics()
			// We expect a queue length of 1, because it's the message the
			// simulator has just sent us, but we haven't acked yet, so
			// for the simulator it's still queued.
			if metrics.Queue != 1 {
				t.Errorf("Expected queue length 1, found %d on connection %d", metrics.Queue, i)
			}
			ack := RawMessage(strings.Join(ACK, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}
		}
		// Deliberately move StartSimulatedTime forward (since that's what
		// main() does) to recreate a bug. Once the Hospital is running,
		// the simulated start time should be recovered from the log, and
		// not the parameter.
		options.StartSimulatedTime = simulatedStarts[i]
		withHospital(options, t, f)
	}
}

func TestRewindOnNewConnections(t *testing.T) {
	mrn := MRN(71880957)
	rt1 := SimulatedTime(time.Date(2024, 02, 11, 14, 15, 0, 0, time.UTC))
	rt2 := SimulatedTime(time.Date(2024, 02, 11, 14, 20, 0, 0, time.UTC))

	now := RealTime(time.Date(2024, 1, 11, 14, 15, 0, 0, time.UTC))
	var lock sync.Mutex

	options := HospitalOptions{
		Messages: []Message{
			{
				MRN:                   mrn,
				Time:                  rt1,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt1,
				RawMessage:            []byte("MSH|...|202402111415|..."),
			},
			{
				MRN:                   mrn,
				Time:                  rt2,
				HasObservation:        true,
				ObservationCreatinine: true,
				ObservationTime:       rt2,
				RawMessage:            []byte("MSH|...|202402111420|..."),
			},
		},
		Now: func() RealTime {
			lock.Lock()
			t := now
			lock.Unlock()
			return t
		},
		AKIs: map[MRN]SimulatedTime{
			mrn: rt1,
		},
		RewindOnNewConnections: true,
		ReplayInRealTime:       false,
	}

	f := func(connect func() *MLLPConnection, pager string, h *Hospital, t *testing.T) {
		for i := 0; i < 2; i++ {
			c := connect()
			raw, err := c.Read()
			if err != nil {
				t.Fatalf("Expected no error, found: %s on connection %d", err, i)
			}
			ack := RawMessage(strings.Join(ACK, "\r") + "\r")
			if err := c.Write(ack); err != nil {
				t.Fatalf("Expected no error, found: %s", err)
			}

			if !bytes.Equal(raw, options.Messages[0].RawMessage) {
				t.Errorf("Unexpected message on connection %d: %s", i, string(raw))
			}
			c.C.Close()
		}
	}
	withHospital(options, t, f)
}
