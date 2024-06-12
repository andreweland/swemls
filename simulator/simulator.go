package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type RawMessage []byte

const (
	MLLP_START_OF_BLOCK  = byte(0x0b)
	MLLP_END_OF_BLOCK    = byte(0x1c)
	MLLP_CARRIAGE_RETURN = byte(0x0d)
)

func fromMLLP(buffer []byte) ([]RawMessage, []byte, error) {
	var messages []RawMessage
	expect := MLLP_START_OF_BLOCK
	message := false
	i := 0
	consumed := 0
	var err error
	for i < len(buffer) {
		if message {
			if buffer[i] == MLLP_END_OF_BLOCK {
				message = false
				expect = MLLP_CARRIAGE_RETURN
			}
		} else {
			if buffer[i] != expect {
				err = fmt.Errorf("bad MLLP encoding: expected %v, found %v", expect, buffer[i])
				break
			}
			switch expect {
			case MLLP_START_OF_BLOCK:
				message = true
				consumed = i
			case MLLP_CARRIAGE_RETURN:
				messages = append(messages, RawMessage(buffer[consumed+1:i-1]))
				expect = MLLP_START_OF_BLOCK
				consumed = i + 1
			}
		}
		i++
	}
	return messages, buffer[consumed:], err
}

func toMLLP(message RawMessage, buffer []byte) []byte {
	buffer = append(buffer, MLLP_START_OF_BLOCK)
	buffer = append(buffer, []byte(message)...)
	buffer = append(buffer, MLLP_END_OF_BLOCK)
	buffer = append(buffer, MLLP_CARRIAGE_RETURN)
	return buffer
}

type MLLPConnection struct {
	C net.Conn

	r        []byte
	w        []byte
	messages []RawMessage
}

func (m *MLLPConnection) Read() (RawMessage, error) {
	start := len(m.r)
	for len(m.messages) < 1 {
		if cap(m.r)-start < MLLPBufferSize {
			m.r = append(m.r, make([]byte, MLLPBufferSize)...)
		} else {
			m.r = m.r[0 : start+MLLPBufferSize]
		}
		m.C.SetDeadline(time.Now().Add(MLLPTimemout))
		n, err := m.C.Read(m.r[start:])
		if err != nil {
			return nil, err
		}
		var messages []RawMessage
		var remaining []byte
		messages, remaining, err = fromMLLP(m.r[0 : start+n])
		for _, message := range messages {
			c := make([]byte, len(message))
			copy(c, message)
			m.messages = append(m.messages, c)
		}
		copy(m.r, remaining[0:]) // Common case: remaining is empty
		m.r = m.r[0:len(remaining)]
		start = len(remaining)
	}
	message := m.messages[0]
	copy(m.messages, m.messages[1:])
	m.messages = m.messages[0 : len(m.messages)-1]
	return message, nil
}

func (m *MLLPConnection) IsMessageReady() bool {
	return len(m.messages) > 0
}

func (m *MLLPConnection) Write(message RawMessage) error {
	m.w = toMLLP(message, m.w[0:0])
	for len(m.w) > 0 {
		m.C.SetDeadline(time.Now().Add(MLLPTimemout))
		n, err := m.C.Write(m.w)
		if err != nil {
			return err
		}
		copy(m.w[0:], m.w[n:])
		m.w = m.w[0 : len(m.w)-n]
	}
	return nil
}

const TimeLayout = "20060102150405"
const TimeLayoutNoSeconds = "200601021504"
const TimeLayoutNoTime = "20060102"

type MRN int
type SimulatedTime time.Time

func (s SimulatedTime) Add(d time.Duration) SimulatedTime {
	return SimulatedTime(time.Time(s).Add(d))
}

func (s SimulatedTime) Before(other SimulatedTime) bool {
	return time.Time(s).Before(time.Time(other))
}

func (s SimulatedTime) After(other SimulatedTime) bool {
	return time.Time(s).After(time.Time(other))
}

func (s SimulatedTime) Format(layout string) string {
	return time.Time(s).Format(layout)
}

func (s SimulatedTime) String() string {
	return time.Time(s).Format(TimeLayout)
}

func SimulatedTimeFromString(s string) (SimulatedTime, error) {
	t, err := RealTimeFromString(s)
	if err != nil {
		return SimulatedTime{}, err
	}
	return SimulatedTime(t), nil
}

type RealTime time.Time

func (r RealTime) Sub(other RealTime) time.Duration {
	return time.Time(r).Sub(time.Time(other))
}

func (r RealTime) Format(layout string) string {
	return time.Time(r).Format(layout)
}

func (r RealTime) IsZero() bool {
	return time.Time(r).IsZero()
}

func (r RealTime) String() string {
	return time.Time(r).Format(TimeLayout)
}

func RealTimeFromString(s string) (RealTime, error) {
	var t time.Time
	var err error
	switch len(s) {
	case len(TimeLayout):
		t, err = time.Parse(TimeLayout, s)
	case len(TimeLayoutNoSeconds):
		t, err = time.Parse(TimeLayoutNoSeconds, s)
	case len(TimeLayoutNoTime):
		t, err = time.Parse(TimeLayoutNoTime, s)
	default:
		err = fmt.Errorf("bad time format: %s", s)
	}
	return RealTime(t), err
}

type Message struct {
	Time                  SimulatedTime
	MRN                   MRN
	HasAck                bool
	AckPositive           bool
	HasObservation        bool
	ObservationTime       SimulatedTime
	ObservationCreatinine bool
	RawMessage
}

type Segment struct {
	Type   string
	Fields [][]byte
}

func parseSegments(m RawMessage) ([]Segment, error) {
	var segments []Segment
	var err error
	if m[len(m)-1] == byte('\r') {
		m = m[0 : len(m)-1]
	}
	for _, segment := range bytes.Split(m, []byte("\r")) {
		fields := bytes.Split(segment, []byte("|"))
		if len(fields) < 1 || len(fields[0]) != 3 {
			err = fmt.Errorf("bad HL7 segment type")
			break
		}
		segments = append(segments, Segment{Type: string(fields[0]), Fields: fields[1:]})
	}
	return segments, err
}

var (
	HL7_MSH_TIME_FIELD = 6

	HL7_MSA_ACK_CODE_FIELD  = 1
	HL7_MSA_ACK_CODE_ACCEPT = []byte("AA")

	HL7_PID_INTERNAL_ID_FIELD = 3

	HL7_OBR_TIME_FIELD = 7

	HL7_OBX_IDENTIFIER_FIELD      = 3
	HL7_OBX_IDENTIFIER_CREATININE = []byte("CREATININE")
)

var errHL7BadTimestamp = errors.New("bad HL7 timestamp")

func parseDTM(buffer []byte) (SimulatedTime, error) {
	var t time.Time
	var err error
	switch len(buffer) {
	case 12:
		t, err = time.Parse("200601021504", string(buffer))
	case 14:
		t, err = time.Parse("20060102150405", string(buffer))
	default:
		err = errHL7BadTimestamp
	}
	return SimulatedTime(t), err
}

func parseMessage(r RawMessage) (Message, error) {
	m := Message{RawMessage: r}
	segments, err := parseSegments(r)
	if err == nil {
		if len(segments) < 1 || segments[0].Type != "MSH" {
			err = errors.New("no MSH segment")
		} else {
			if len(segments[0].Fields) >= HL7_MSH_TIME_FIELD {
				m.Time, _ = parseDTM(segments[0].Fields[HL7_MSH_TIME_FIELD-1])
			}
			for _, segment := range segments[1:] {
				switch segment.Type {
				case "PID":
					if len(segment.Fields) >= HL7_PID_INTERNAL_ID_FIELD {
						if mrn, err := strconv.Atoi(string(segment.Fields[HL7_PID_INTERNAL_ID_FIELD-1])); err == nil {
							m.MRN = MRN(mrn)
						} else {
							return m, fmt.Errorf("bad PID segment: bad internal ID: %w", err)
						}
					}
				case "MSA":
					if len(segment.Fields) >= HL7_MSA_ACK_CODE_FIELD {
						m.HasAck = true
						m.AckPositive = bytes.Equal(segment.Fields[HL7_MSA_ACK_CODE_FIELD-1], HL7_MSA_ACK_CODE_ACCEPT)
					}
				case "OBR":
					if len(segment.Fields) >= HL7_OBR_TIME_FIELD {
						m.ObservationTime, err = parseDTM(segment.Fields[HL7_OBR_TIME_FIELD-1])
						m.HasObservation = err == nil
					}
				case "OBX":
					if len(segment.Fields) >= HL7_OBX_IDENTIFIER_FIELD {
						m.ObservationCreatinine = bytes.Equal(segment.Fields[HL7_OBX_IDENTIFIER_FIELD-1], HL7_OBX_IDENTIFIER_CREATININE)
					}
				}
			}
		}
	}
	return m, nil
}

func readMessagesFromFile(filename string) ([]Message, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ms []Message
	remaining := make([]byte, 0)
	buffer := make([]byte, 8096)
	for {
		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		remaining = append(remaining, buffer[0:n]...)
		var rs []RawMessage
		rs, remaining, err = fromMLLP(remaining)
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			m, err := parseMessage(r)
			if err != nil {
				return nil, err
			}
			ms = append(ms, m)
		}
	}
	return ms, nil
}

func isColumnTrue(column string) bool {
	return column == "1" || column == "y"
}

type AKIs struct {
	AKIs            map[MRN]SimulatedTime
	NHSDetectedAKIs map[MRN]SimulatedTime
	NHSMetrics      ModelMetrics
}

func readAKIsFromFile(filename string) (AKIs, error) {
	akis := AKIs{
		AKIs:            make(map[MRN]SimulatedTime),
		NHSDetectedAKIs: make(map[MRN]SimulatedTime),
	}

	f, err := os.Open(filename)
	if err != nil {
		return akis, err
	}

	r := csv.NewReader(f)
	headers, err := r.Read()
	if err != nil {
		return akis, err
	}
	dateColumn := -1
	mrnColumn := -1
	akiColumn := -1
	nhsColumn := -1
	for i, header := range headers {
		switch header {
		case "mrn":
			mrnColumn = i
		case "date":
			dateColumn = i
		case "aki":
			akiColumn = i
		case "nhs":
			nhsColumn = i
		}
	}
	if dateColumn < 0 || mrnColumn < 0 {
		return akis, fmt.Errorf("%s: bad headers", filename)
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return akis, err
		}
		mrn, err := strconv.Atoi(row[mrnColumn])
		if err != nil {
			return akis, fmt.Errorf("%s: bad mrn: %s", filename, row[mrnColumn])
		}
		t, err := time.Parse("2006-01-02 15:04:05", row[dateColumn])
		if err != nil {
			return akis, fmt.Errorf("%s: bad date: %s", filename, row[dateColumn])
		}
		aki := akiColumn < 0 || isColumnTrue(row[akiColumn])
		if aki {
			akis.AKIs[MRN(mrn)] = SimulatedTime(t)
		}
		if nhsColumn >= 0 {
			if isColumnTrue(row[nhsColumn]) {
				akis.NHSDetectedAKIs[MRN(mrn)] = SimulatedTime(t)
				if aki {
					akis.NHSMetrics.TruePositives++
				} else {
					akis.NHSMetrics.FalsePositives++
				}
			} else {
				if aki {
					akis.NHSMetrics.FalseNegatives++
				}
			}
		}
	}
	return akis, nil
}

type MetricLabels map[string]string

func (m MetricLabels) String() string {
	if len(m) == 0 {
		return ""
	}
	l := "{"
	first := true
	for k, v := range m {
		if first {
			first = false
		} else {
			l += ","
		}
		l += fmt.Sprintf("%s=%q", k, v)
	}
	return l + "}"
}

func precision(tp int, fp int) float64 {
	if tp+fp == 0 {
		return 0.0
	}
	return float64(tp) / float64(tp+fp)
}

func recall(tp int, fn int) float64 {
	if tp+fn == 0 {
		return 0.0
	}
	return float64(tp) / float64(tp+fn)
}

func fbscore(tp int, fp int, fn int, b float64) float64 {
	p := precision(tp, fp)
	r := recall(tp, fn)
	if p == 0.0 && r == 0.0 {
		return 0.0
	}
	return (1.0 + (b * b)) * ((p * r) / (((b * b) * p) + r))
}

type ModelMetrics struct {
	FalsePositives int
	TruePositives  int
	FalseNegatives int
}

func (m *ModelMetrics) F3Score() float64 {
	return fbscore(m.TruePositives, m.FalsePositives, m.FalseNegatives, 3.0)
}

func (m *ModelMetrics) ResetCounts() {
	m.FalsePositives = 0
	m.TruePositives = 0
	m.FalseNegatives = 0
}

func (m *ModelMetrics) Write(prefix string, labels MetricLabels, w io.Writer) {
	w.Write([]byte(fmt.Sprintf("%strue_positives%s %d\n", prefix, labels, m.TruePositives)))
	w.Write([]byte(fmt.Sprintf("%sfalse_positives%s %d\n", prefix, labels, m.FalsePositives)))
	w.Write([]byte(fmt.Sprintf("%sfalse_negatives%s %d\n", prefix, labels, m.FalseNegatives)))
	w.Write([]byte(fmt.Sprintf("%sf3_score%s %f\n", prefix, labels, m.F3Score())))
}

type Metrics struct {
	Pages          ModelMetrics
	NHS            ModelMetrics
	MessagesSent   int
	ResultsSent    int
	Queue          int
	Latency        float64
	Latency50      float64
	Latency90      float64
	LastError      error
	Connected      bool
	ConnectionTime time.Time
}

func (m *Metrics) ResetCounts() {
	m.Pages.ResetCounts()
	m.NHS.ResetCounts()
	m.MessagesSent = 0
	m.ResultsSent = 0
	m.Latency50 = 0.0
	m.Latency90 = 0.0
}

func (m *Metrics) Write(labels MetricLabels, w io.Writer) {
	m.Pages.Write("", labels, w)
	m.NHS.Write("nhs_", labels, w)
	w.Write([]byte(fmt.Sprintf("messages_sent%s %d\n", labels, m.MessagesSent)))
	w.Write([]byte(fmt.Sprintf("results_sent%s %d\n", labels, m.ResultsSent)))
	w.Write([]byte(fmt.Sprintf("queue%s %d\n", labels, m.Queue)))
	w.Write([]byte(fmt.Sprintf("latency%s %f\n", labels, m.Latency)))
	w.Write([]byte(fmt.Sprintf("latencies_50%s %f\n", labels, m.Latency50)))
	w.Write([]byte(fmt.Sprintf("latencies_90%s %f\n", labels, m.Latency90)))
	connections := 0
	if m.Connected {
		connections = 1
	}
	w.Write([]byte(fmt.Sprintf("connections%s %d\n", labels, connections)))
}

const MLLPBufferSize = 1024
const MLLPAcceptQueueLength = 4
const MLLPTimemout = 10 * time.Second
const ShutdownTimeout = 10 * time.Second

var MissedPageTime = SimulatedTime(time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC))

type EventType string

const (
	EventTypeRewind  EventType = "Rewind"
	EventTypeSend    EventType = "Send"
	EventTypeReceive EventType = "Receive"
	EventTypePage    EventType = "Page"
	EventTypeInfo    EventType = "Info"
	EventTypeError   EventType = "Error"
)

type Event struct {
	Type          EventType
	SimulatedTime SimulatedTime
	RealTime      RealTime
	Index         int // Either an index into Hospital.Messages, or an MRN
	Detail        string
}

func (e *Event) String() string {
	return fmt.Sprintf("%s %s %s %d", e.Type, e.SimulatedTime, e.RealTime, e.Index)
}

func (e *Event) FromString(s string) error {
	var st string
	var rt string
	_, err := fmt.Sscanf(s, "%s %s %s %d", &e.Type, &st, &rt, &e.Index)
	if err == nil {
		e.SimulatedTime, err = SimulatedTimeFromString(st)
		if err == nil {
			e.RealTime, err = RealTimeFromString(rt)
		}
	}
	return err
}

const RecentEventLogSize = 200

type EventLog struct {
	filename string
	ctx      context.Context

	c      chan *Event
	w      *os.File
	recent []Event
	i      int
	lock   sync.Mutex
}

func NewEventLog(filename string, ctx context.Context) *EventLog {
	// Use a channel with buffer 1, as an initial rewind event will be
	// logged before the log itself is running.
	return &EventLog{filename: filename, ctx: ctx, c: make(chan *Event, 1)}
}

func (e *EventLog) Replay(replay func(*Event) error) error {
	if e.filename == "" {
		return nil
	}

	f, err := os.Open(e.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer f.Close()
	s := bufio.NewScanner(f)
	event := Event{}
	for s.Scan() {
		if err := event.FromString(s.Text()); err != nil {
			return fmt.Errorf("event log: from string: %w", err)
		}
		if err := replay(&event); err != nil {
			return fmt.Errorf("event log: replay: %w", err)
		}
	}
	return nil
}

func (e *EventLog) Run() error {
	if e.filename != "" {
		var err error
		e.w, err = os.OpenFile(e.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("can't write: %s", err)
			return err
		}
		return e.writeEvents()
	}
	return nil
}

func (e *EventLog) Log(event *Event) {
	if e == nil {
		return
	}

	e.lock.Lock()
	if len(e.recent) < RecentEventLogSize {
		e.recent = append(e.recent, *event)
	} else {
		e.recent[e.i] = *event
		e.i = (e.i + 1) % RecentEventLogSize
	}
	e.lock.Unlock()

	if e.filename != "" {
		select {
		case e.c <- event:
		case <-e.ctx.Done():
		}
	}
}

func (e *EventLog) RecentEvents() []Event {
	e.lock.Lock()
	defer e.lock.Unlock()
	recent := make([]Event, 0, RecentEventLogSize)
	if len(e.recent) < RecentEventLogSize {
		copy(recent, e.recent)
	} else {
		i := e.i
		start := e.i
		for {
			recent = append(recent, e.recent[i])
			i = (i + 1) % RecentEventLogSize
			if i == start {
				break
			}
		}
	}
	return recent
}

const EventLogSyncInterval = 1 * time.Second

func (e *EventLog) writeEvents() error {
	var buffer bytes.Buffer
	t := time.NewTimer(EventLogSyncInterval)
	t.Stop()
Done:
	for {
	Sync:
		for {
			select {
			case event := <-e.c:
				if err := e.writeEvent(event, &buffer); err != nil {
					return fmt.Errorf("event log: write: %w", err)
				}
				t.Reset(EventLogSyncInterval)
			case <-t.C:
				break Sync
			case <-e.ctx.Done():
				close(e.c)
				break Done
			}
		}
		e.w.Sync()
	}
	for event := range e.c {
		if err := e.writeEvent(event, &buffer); err != nil {
			return fmt.Errorf("event log: final write: %w", err)
		}
	}
	e.w.Sync()
	return e.w.Close()
}

func (e *EventLog) writeEvent(event *Event, buffer *bytes.Buffer) error {
	buffer.Reset()
	buffer.WriteString(event.String())
	buffer.WriteByte('\n')
	if _, err := buffer.WriteTo(e.w); err != nil {
		e.w.Close()
		return err
	}
	return nil
}

type HospitalOptions struct {
	Name                   string
	MLLPAddr               string
	PagerAddr              string
	Messages               []Message
	AKIs                   map[MRN]SimulatedTime
	NHSDetectedAKIs        map[MRN]SimulatedTime
	ReplayInRealTime       bool
	RewindOnNewConnections bool
	EventLogFilename       string
	Context                context.Context
	// The simulator's time when it's created. Only used if state is not
	// recovered from the event log. If state is recovered, a start time
	// is computed based on the real to simulated time delta at the
	// original start.
	StartSimulatedTime SimulatedTime
	Now                func() RealTime
	Log                *log.Logger
}

type Hospital struct {
	name     string
	context  context.Context
	now      func() RealTime
	messages []Message
	akis     map[MRN]SimulatedTime

	// TODO: remove
	replayInRealTime       bool
	rewindOnNewConnections bool
	nhsDetectedAKIs        map[MRN]SimulatedTime

	startSimulatedTime SimulatedTime
	startRealTime      RealTime

	lock            sync.Mutex
	nextMessage     int
	mllpListener    net.Listener
	pager           *http.Server
	mllpConnections chan *MLLPConnection

	eventLog   *EventLog
	logContext context.Context
	logCancel  func()

	lastResultSimulatedTime map[MRN]SimulatedTime
	resultRealTime          map[SimulatedTime]RealTime
	pages                   map[MRN]SimulatedTime
	metrics                 Metrics
	latencies               []float64

	log *log.Logger
}

func NewHospital(options *HospitalOptions) (*Hospital, error) {
	h := &Hospital{
		name:                   options.Name,
		context:                options.Context,
		now:                    options.Now,
		messages:               options.Messages,
		akis:                   options.AKIs,
		replayInRealTime:       options.ReplayInRealTime,
		startSimulatedTime:     options.StartSimulatedTime,
		rewindOnNewConnections: options.RewindOnNewConnections,
		nhsDetectedAKIs:        options.NHSDetectedAKIs,
		log:                    options.Log,
	}

	if h.now == nil {
		h.now = func() RealTime { return RealTime(time.Now()) }
	}

	h.mllpConnections = make(chan *MLLPConnection, MLLPAcceptQueueLength)

	h.logContext, h.logCancel = context.WithCancel(context.Background())
	h.eventLog = NewEventLog(options.EventLogFilename, h.logContext)

	var err error
	h.mllpListener, err = net.Listen("tcp", options.MLLPAddr)
	if err != nil {
		h.log.Printf("mllp: %s", err)
		h.logCancel()
		return nil, err
	}

	h.pager = &http.Server{
		Addr:    options.PagerAddr,
		Handler: h,
	}

	h.log.Printf("%s: mllp: %s pager: %s", options.Name, options.MLLPAddr, options.PagerAddr)

	if options.EventLogFilename != "" {
		f := func(event *Event) error {
			return h.replayEvent(event)
		}
		if err := h.eventLog.Replay(f); err != nil {
			h.logCancel()
			return nil, err
		}
	}

	if h.startRealTime.IsZero() { // A fresh start - no events were replayed
		h.handleEvent(&Event{Type: EventTypeRewind, SimulatedTime: h.startSimulatedTime})
	}

	if h.log == nil {
		h.log = log.Default()
	}

	return h, nil
}

func (h *Hospital) Name() string {
	return h.name
}

func (h *Hospital) Run() error {
	logGroup, logContext := errgroup.WithContext(h.logContext)
	logGroup.Go(func() error { return h.eventLog.Run() })

	g, ctx := errgroup.WithContext(h.context)
	g.Go(func() error { return h.serveMLLPConnections() })
	g.Go(func() error { return h.acceptMLLPConnections() })
	g.Go(func() error {
		err := h.pager.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("pager: %w", err)
	})
	select {
	case <-ctx.Done():
	case <-logContext.Done():
	}
	h.mllpListener.Close()
	timeout, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	h.pager.Shutdown(timeout)
	cancel()
	err := g.Wait()
	h.logCancel() // Shutdown the log last to ensure it drains log events
	if logErr := logGroup.Wait(); logErr != nil {
		if err != nil {
			h.log.Printf("event log: %s", logErr)
		} else {
			err = logErr
		}
	}
	return err
}

func (h *Hospital) Rewind() {
	h.handleEvent(&Event{Type: EventTypeRewind, SimulatedTime: h.startSimulatedTime})
}

func (h *Hospital) Disconnect() {
	h.mllpConnections <- nil
}

func (h *Hospital) Metrics() Metrics {
	h.lock.Lock()
	defer h.lock.Unlock()
	metrics := h.metrics
	sort.Float64s(h.latencies)
	if len(h.latencies) > 0 {
		metrics.Latency50 = h.latencies[(len(h.latencies)*50)/100]
		metrics.Latency90 = h.latencies[(len(h.latencies)*90)/100]
	}
	if h.replayInRealTime {
		t := h.simulatedNow()
		metrics.Queue = sort.Search(len(h.messages)-h.nextMessage, func(j int) bool {
			return h.messages[h.nextMessage+j].Time.After(t)
		})
	} else {
		metrics.Queue = len(h.messages) - h.nextMessage
	}
	return metrics
}

func (h *Hospital) RecentEvents() []Event {
	return h.eventLog.RecentEvents()
}

func (h *Hospital) handleEvent(event *Event) error {
	event.RealTime = h.now()
	h.eventLog.Log(event)
	return h.replayEvent(event)
}

func (h *Hospital) replayEvent(event *Event) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	switch event.Type {
	case EventTypeRewind:
		h.nextMessage = 0
		h.metrics.ResetCounts()
		h.startSimulatedTime = event.SimulatedTime
		h.startRealTime = event.RealTime
		h.lastResultSimulatedTime = make(map[MRN]SimulatedTime)
		h.resultRealTime = make(map[SimulatedTime]RealTime)
		h.pages = make(map[MRN]SimulatedTime)
	case EventTypeSend:
		h.nextMessage = event.Index + 1
		m := h.messages[event.Index]
		h.metrics.MessagesSent++
		if m.HasObservation && m.ObservationCreatinine {
			h.lastResultSimulatedTime[m.MRN] = m.ObservationTime
			h.resultRealTime[m.ObservationTime] = event.RealTime
			h.metrics.ResultsSent++
			aki := false
			if expected, ok := h.akis[m.MRN]; ok {
				aki = expected == m.ObservationTime
				if m.ObservationTime.After(expected) {
					if _, ok := h.pages[m.MRN]; !ok {
						h.metrics.Pages.FalseNegatives++
						h.pages[m.MRN] = MissedPageTime
					}
				}
			}
			if detected, ok := h.nhsDetectedAKIs[m.MRN]; ok {
				if detected == m.ObservationTime {
					if aki {
						h.metrics.NHS.TruePositives++
					} else {
						h.metrics.NHS.FalsePositives++
					}
				} else if aki {
					h.metrics.NHS.FalseNegatives++
				}
			} else if aki {
				h.metrics.NHS.FalseNegatives++
			}
		}
	case EventTypeReceive:
	case EventTypePage:
		existing, existingOK := h.pages[MRN(event.Index)]
		expected, expectedOK := h.akis[MRN(event.Index)]
		if expectedOK {
			if expected == event.SimulatedTime {
				if existingOK {
					if existing != expected {
						h.metrics.Pages.TruePositives++
						h.metrics.Pages.FalseNegatives--
					} // Otherwise we've already counted the true positive
				} else {
					h.metrics.Pages.TruePositives++
				}
			} else {
				if !existingOK {
					h.metrics.Pages.FalsePositives++
				} // Otherwise we've already counted the false negative
			}
		} else {
			if !existingOK || existing != event.SimulatedTime {
				h.metrics.Pages.FalsePositives++
			}
		}
		h.pages[MRN(event.Index)] = event.SimulatedTime

		if sent, ok := h.resultRealTime[event.SimulatedTime]; ok {
			h.metrics.Latency = event.RealTime.Sub(sent).Seconds()
			h.latencies = append(h.latencies, h.metrics.Latency)
		}
	}
	return nil
}

func (h *Hospital) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/page" {
		if body, err := io.ReadAll(r.Body); err == nil {
			if mrn, t, err := h.parsePageBody(body); err == nil {
				h.log.Printf("%s: paging for MRN %d at %s", h.name, mrn, t)
				h.handleEvent(&Event{Type: EventTypePage, Index: int(mrn), SimulatedTime: t})
				w.Header().Add("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok\n"))
				return
			}
		}
	} else if r.URL.Path == "/metrics" {
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		m := h.Metrics()
		m.Write(nil, w)
		return
	}
	http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
}

var (
	errPageBadMRN       error = errors.New("bad MRN")
	errPageBadTimestamp error = errors.New("bad timestamp")
	errPageTooManyParts error = errors.New("too many parts in body")
)

func (h *Hospital) parsePageBody(body []byte) (MRN, SimulatedTime, error) {
	var mrn int
	var t SimulatedTime
	var err error
	parts := bytes.Split(body, []byte{','})
	if len(parts) < 3 {
		mrn, err = strconv.Atoi(string(parts[0]))
		if err == nil {
			if len(parts) > 1 {
				t, err = parseDTM(parts[1])
				if err != nil {
					err = errPageBadTimestamp
				}
			} else {
				h.lock.Lock()
				rt, ok := h.lastResultSimulatedTime[MRN(mrn)]
				h.lock.Unlock()
				if ok {
					t = rt
				}
			}
		} else {
			err = errPageBadMRN
		}
	} else {
		err = errPageTooManyParts
	}
	return MRN(mrn), t, err
}

func (h *Hospital) acceptMLLPConnections() error {
	defer close(h.mllpConnections)
	for {
		c, err := h.mllpListener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("mllp: accept: %w", err)
		}
		h.mllpConnections <- &MLLPConnection{C: c}
	}
}

func (h *Hospital) serveMLLPConnections() error {
	var next *MLLPConnection
	for {
		h.lock.Lock()
		h.metrics.Connected = false
		h.lock.Unlock()
		if next == nil {
			var ok bool
			next, ok = <-h.mllpConnections
			if !ok {
				break
			}
			if next == nil { // Forced disconnect
				continue
			}
		}
		h.lock.Lock()
		h.metrics.Connected = true
		h.metrics.ConnectionTime = time.Now()
		h.lock.Unlock()
		if h.rewindOnNewConnections {
			h.handleEvent(&Event{Type: EventTypeRewind})
		}
		addr := next.C.RemoteAddr()
		h.log.Printf("%s: mllp: connected to %s", h.name, addr)
		next, h.metrics.LastError = h.serveMLLPConnection(next)
		if h.metrics.LastError != nil {
			h.handleEvent(&Event{Type: EventTypeError, Detail: h.metrics.LastError.Error()})
			h.log.Printf("%s: mllp: %s", h.name, h.metrics.LastError)
		} else {
			h.log.Printf("%s: mllp: disconnected from %s", h.name, addr)
		}
	}
	return nil
}

func (h *Hospital) serveMLLPConnection(c *MLLPConnection) (*MLLPConnection, error) {
	defer c.C.Close()
	for {
		h.lock.Lock()
		i := h.nextMessage
		h.lock.Unlock()
		if i >= len(h.messages) {
			break
		}
		m := h.messages[i]
		if h.replayInRealTime {
			timer := time.NewTimer(time.Time(m.Time).Sub(time.Time(h.simulatedNow())))
			select {
			case next := <-h.mllpConnections:
				return next, nil
			case <-timer.C:
			}
		} else {
			select {
			case next := <-h.mllpConnections:
				return next, nil
			default:
			}
		}

		// Allow for pages before a message is acked
		if m.HasObservation && m.ObservationCreatinine {
			h.lastResultSimulatedTime[m.MRN] = m.ObservationTime
			h.resultRealTime[m.ObservationTime] = h.now()
		}

		if err := c.Write(m.RawMessage); errors.Is(err, io.EOF) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		raw, err := c.Read()
		if err == io.EOF {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		h.handleEvent(&Event{Type: EventTypeSend, Index: h.nextMessage, SimulatedTime: m.Time})

		if c.IsMessageReady() {
			return nil, fmt.Errorf("mllp: multiple MSA messages")
		}
		am, err := parseMessage(raw)
		if err != nil {
			return nil, err
		}
		if am.HasAck {
			if am.AckPositive {
				i++
			}
		} else {
			return nil, fmt.Errorf("mllp: bad acknowledgement: %q", string(raw[0]))
		}
	}
	return nil, nil
}

func (h *Hospital) simulatedNow() SimulatedTime {
	return h.startSimulatedTime.Add(time.Time(h.now()).Sub(time.Time(h.startRealTime)))
}

type BackstageUI struct {
	Hospitals []*Hospital
	Groups    []Group
	Addr      string
	Directory string
	Context   context.Context

	template *template.Template
	server   *http.Server
}

func (b *BackstageUI) Run() error {
	var err error
	b.template, err = template.ParseGlob(filepath.Join(b.Directory, "*.html"))
	if err != nil {
		return err
	}

	handler := http.NewServeMux()
	handler.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.serveUI(w, r)
	}))
	handler.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.serveMetrics(w, r)
	}))
	handler.Handle("/simulator.css", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(b.Directory, "simulator.css"))
	}))

	b.server = &http.Server{
		Addr:    b.Addr,
		Handler: handler,
	}
	log.Printf("backstage: %s", b.Addr)
	g, ctx := errgroup.WithContext(b.Context)
	g.Go(func() error {
		err := b.server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("backstage: %w", err)
	})
	<-ctx.Done()
	timeout, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	b.server.Shutdown(timeout)
	cancel()
	return g.Wait()
}

type htmlGroup struct {
	Metrics       Metrics
	Group         Group
	ConnectedTime string
}

type html struct {
	Title   string
	Groups  []htmlGroup
	Group   *Group
	Metrics *Metrics
	Events  []Event
}

func (b *BackstageUI) serveUI(w http.ResponseWriter, r *http.Request) {
	groupPrefix := "/group/"
	if strings.HasPrefix(r.URL.Path, groupPrefix) {
		group := r.URL.Path[len(groupPrefix):]
		for i, hospital := range b.Hospitals {
			if hospital.Name() == group {
				b.serveGroupUI(&b.Groups[i], hospital, w, r)
				return
			}
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	} else {
		b.serveIndexUI(w, r)
	}
}

func (b *BackstageUI) serveIndexUI(w http.ResponseWriter, r *http.Request) {
	var h html
	h.Title = "Simulated Hospital"
	h.Groups = make([]htmlGroup, len(b.Hospitals))
	for i := range b.Hospitals {
		h.Groups[i].Metrics = b.Hospitals[i].Metrics()
		h.Groups[i].Group = b.Groups[i]
		if h.Groups[i].Metrics.Connected {
			t := int(time.Now().Sub(h.Groups[i].Metrics.ConnectionTime).Seconds())
			h.Groups[i].ConnectedTime = ""
			if t > 60*60 {
				h.Groups[i].ConnectedTime += fmt.Sprintf("%dh", t/(60*60))
				t = t % (60 * 60)
			}
			if t > 60 {
				h.Groups[i].ConnectedTime += fmt.Sprintf("%dm", t/60)
				t = t % 60
			}
			h.Groups[i].ConnectedTime += fmt.Sprintf("%ds", t)
		}
	}
	w.Header().Set("Content-Type", "text/html")
	b.template.Execute(w, h)
}

func (b *BackstageUI) serveGroupUI(group *Group, hospital *Hospital, w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		if err := r.ParseForm(); err == nil {
			switch r.PostFormValue("e") {
			case "rewind":
				hospital.Rewind()
				http.Redirect(w, r, r.URL.String(), http.StatusSeeOther)
			case "disconnect":
				hospital.Disconnect()
				http.Redirect(w, r, r.URL.String(), http.StatusSeeOther)
			}
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var h html
	h.Title = group.Name
	h.Group = group
	metrics := hospital.Metrics()
	h.Metrics = &metrics
	h.Events = hospital.RecentEvents()
	for i := 0; i < len(h.Events)/2; i++ {
		h.Events[len(h.Events)-i-1], h.Events[i] = h.Events[i], h.Events[len(h.Events)-i-1]
	}
	w.Header().Set("Content-Type", "text/html")
	b.template.Execute(w, h)
}

func (b *BackstageUI) serveMetrics(w http.ResponseWriter, r *http.Request) {
	labels := map[string]string{"group": ""}
	for _, h := range b.Hospitals {
		metrics := h.Metrics()
		labels["group"] = h.Name()
		metrics.Write(labels, w)
	}
}

type Group struct {
	Name  string `yaml:"name"`
	MLLP  int    `yaml:"mllp"`
	Pager int    `yaml:"pager"`
}

type Groups struct {
	Groups []Group `yaml:"groups"`
}

func main() {
	messagesFlag := flag.String("messages", "messages.mllp", "HL7 messages to replay, in MLLP format")
	postProcessedMessagesFlag := flag.String("post-processed-messages", "", "Post processed messages for content replacement")
	akisFlag := flag.String("akis", "akis.csv", "Expected AKI events")
	mllpFlag := flag.Int("mllp", 8440, "Port on which to replay HL7 messages via MLLP")
	pagerFlag := flag.Int("pager", 8441, "Port on which to listen for pager requests via HTTP")
	backstageFlag := flag.Int("backstage", 9001, "Port on which to serve the backstage UI")
	addrFlag := flag.String("addr", "0.0.0.0", "Address on which to bind")
	groupsFlag := flag.String("groups", "", "Group assignments")
	stateFlag := flag.String("state", "", "Save simulator state to this directory")
	realtimeFlag := flag.Bool("realtime", false, "Replay messages in real time")
	htmlFlag := flag.String("html", ".", "Directory for HTML content")
	realTimeFlag := flag.String("real-time", "", "Real reference time, for calculating simulated start time")
	simulatedTimeFlag := flag.String("simulated-time", "", "Simulated reference time, for calculating simulated start time")
	rewindFlag := flag.Bool("rewind", true, "Rewind on new connections")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)

	messages, err := readMessagesFromFile(*messagesFlag)
	if err != nil {
		log.Fatal(err)
	}

	if *postProcessedMessagesFlag != "" {
		postProcessedMessages, err := readMessagesFromFile(*postProcessedMessagesFlag)
		if err != nil {
			log.Fatal(err)
		}
		if len(postProcessedMessages) != len(messages) {
			log.Fatalf("%s and %s differ in number of messages", *messagesFlag, *postProcessedMessagesFlag)
		}
		for i := range messages {
			messages[i].RawMessage = postProcessedMessages[i].RawMessage
		}
		log.Printf("hospital: using post processed messages")
	}

	if len(messages) == 0 {
		log.Fatalf("No messages in %s", *messagesFlag)
	}

	if *stateFlag != "" {
		if f, err := os.Stat(*stateFlag); err == nil {
			if !f.IsDir() {
				log.Fatalf("%s: not a directory", *stateFlag)
			}
		} else {
			log.Fatalf("%s: %s", *stateFlag, err)
		}
	}

	start := messages[0].Time
	if *realTimeFlag != "" && *simulatedTimeFlag != "" {
		realTime, err := RealTimeFromString(*realTimeFlag)
		if err != nil {
			log.Fatalf("failed to parse --real-time: %s", err)
		}
		simulatedTime, err := RealTimeFromString(*simulatedTimeFlag)
		if err != nil {
			log.Fatalf("failed to parse --simulated-time: %s", err)
		}
		delta := simulatedTime.Sub(realTime)
		start = SimulatedTime(time.Now()).Add(delta)
		log.Printf("hospital: starting at reference time %s", start)
	} else {
		// Start in the morning of the first day with a blood test results, to
		// ensure a reasonable flow of messages immediately.
		for _, m := range messages {
			if m.HasObservation {
				year, month, day := time.Time(m.Time).Date()
				start = SimulatedTime(time.Date(year, month, day, 7, 0, 0, 0, time.UTC))
				break
			}
		}
		log.Printf("hospital: starting at first blood test result %s", start)
	}

	akis, err := readAKIsFromFile(*akisFlag)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("hospital: %d messages", len(messages))
	log.Printf("hospital: %d akis", len(akis.AKIs))
	log.Printf("hospital: NHS f3 %f", akis.NHSMetrics.F3Score())

	var groups Groups
	if *groupsFlag == "" {
		groups.Groups = []Group{
			{
				Name:  "hospital",
				MLLP:  *mllpFlag,
				Pager: *pagerFlag,
			},
		}
	} else {
		f, err := os.Open(*groupsFlag)
		if err == nil {
			var b []byte
			if b, err = io.ReadAll(f); err == nil {
				err = yaml.Unmarshal(b, &groups)
			}
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("groups: %d", len(groups.Groups))
	}

	hospitals := make([]*Hospital, len(groups.Groups))
	for i := range hospitals {
		var logFilename string
		if *stateFlag != "" {
			logFilename = filepath.Join(*stateFlag, fmt.Sprintf("%s.log", groups.Groups[i].Name))
		}
		options := HospitalOptions{
			Name:                   groups.Groups[i].Name,
			MLLPAddr:               fmt.Sprintf("%s:%d", *addrFlag, groups.Groups[i].MLLP),
			PagerAddr:              fmt.Sprintf("%s:%d", *addrFlag, groups.Groups[i].Pager),
			Messages:               messages,
			AKIs:                   akis.AKIs,
			NHSDetectedAKIs:        akis.NHSDetectedAKIs,
			StartSimulatedTime:     start,
			ReplayInRealTime:       *realtimeFlag,
			RewindOnNewConnections: *rewindFlag,
			EventLogFilename:       logFilename,
			Context:                ctx,
		}
		hospitals[i], err = NewHospital(&options)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := range hospitals {
		i := i
		g.Go(func() error {
			return hospitals[i].Run()
		})
	}

	backstage := BackstageUI{
		Addr:      fmt.Sprintf("%s:%d", *addrFlag, *backstageFlag),
		Hospitals: hospitals,
		Groups:    groups.Groups,
		Directory: *htmlFlag,
		Context:   ctx,
	}
	g.Go(func() error {
		return backstage.Run()
	})

	signals := make(chan os.Signal)
	g.Go(func() error {
		select {
		case s := <-signals:
			log.Printf("simulator: caught %s", s)
			cancel()
		case <-ctx.Done():
		}
		return nil
	})
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}
