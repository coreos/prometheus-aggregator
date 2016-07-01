package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/pkg/flagutil"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/Sirupsen/logrus"
)

var listen = flag.String("listen", ":9092", "")
var level = flag.String("loglevel", "info", "default log level: debug, info, warn, error, fatal, panic")

func main() {
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err := flagutil.SetFlagsFromEnv(flag.CommandLine, "PROMETHEUS_AGGREGATOR"); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	lvl, err := log.ParseLevel(*level)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	log.SetLevel(lvl)

	http.Handle("/metrics", prometheus.Handler())
	http.HandleFunc("/call", wrap(call))
	log.Infoln("listening on", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

func wrap(f func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := f(w, r)
		if err != nil {
			http.Error(w, err.Error(), 500)
		}
	}
}

type options struct {
	// common
	identifier
	Help       string
	LabelNames []string

	// summary
	Objectives map[float64]float64
	MaxAge     time.Duration
	AgeBuckets uint32
	BufCap     uint32

	// histogram
	Buckets []float64
}

func (o *options) register() error {
	lock.RLock()
	mv, ok := metrics[o.identifier]
	lock.RUnlock()

	if ok {
		return nil
	}
	switch o.Type {
	case "Gauge":
		mv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: o.Namespace,
			Subsystem: o.Subsystem,
			Name:      o.Name,
			Help:      o.Help,
		}, o.LabelNames)
	case "Counter":
		mv = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: o.Namespace,
			Subsystem: o.Subsystem,
			Name:      o.Name,
			Help:      o.Help,
		}, o.LabelNames)
	case "Untyped":
		mv = prometheus.NewUntypedVec(prometheus.UntypedOpts{
			Namespace: o.Namespace,
			Subsystem: o.Subsystem,
			Name:      o.Name,
			Help:      o.Help,
		}, o.LabelNames)
	case "Summary":
		mv = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  o.Namespace,
			Subsystem:  o.Subsystem,
			Name:       o.Name,
			Help:       o.Help,
			Objectives: o.Objectives,
			MaxAge:     o.MaxAge,
			AgeBuckets: o.AgeBuckets,
			BufCap:     o.BufCap,
		}, o.LabelNames)
	case "Histogram":
		mv = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: o.Namespace,
			Subsystem: o.Subsystem,
			Name:      o.Name,
			Help:      o.Help,
			Buckets:   o.Buckets,
		}, o.LabelNames)
	default:
		return fmt.Errorf("unknown type: %s", o.Type)
	}
	err := prometheus.Register(mv)
	if err != nil {
		// TODO(mjibson): cache the error instead of creating new collectors each time
		return err
	}
	lock.Lock()
	metrics[o.identifier] = mv
	lock.Unlock()
	return nil
}

type message struct {
	Call string
	Data json.RawMessage
}

func call(w http.ResponseWriter, r *http.Request) error {
	dec := json.NewDecoder(r.Body)
	var me MultiError
	for {
		var m message
		var err error
		if err = dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		log.Debugln(m.Call, string(m.Data))
		switch m.Call {
		case "put":
			err = put(m.Data)
		case "register":
			err = register(m.Data)
		default:
			err = fmt.Errorf("unknown call: %s", m.Call)
		}
		me = append(me, err)
	}
	return me.AsError()
}

func register(msg json.RawMessage) error {
	var o options
	if err := json.Unmarshal(msg, &o); err != nil {
		return err
	}
	return o.register()
}

func put(msg json.RawMessage) error {
	var m metric
	if err := json.Unmarshal(msg, &m); err != nil {
		return err
	}
	return m.put()
}

var (
	lock    sync.RWMutex
	metrics = make(map[identifier]prometheus.Collector)
)

type identifier struct {
	Namespace string
	Subsystem string
	Name      string
	Type      string
}

type metric struct {
	identifier

	LabelValues []string
	// Method is the method name: Set, Inc, Dec, Add, Sub, Observe (dependant on the Type).
	Method string
	// Value is the parameter to Method. Unused for Inc and Dec.
	Value float64
}

const (
	errUnexpectedType = "unknown type %T: expected %s"
	errUnknownMethod  = "unknown method %s on type %s"
)

func (m *metric) put() error {
	lock.RLock()
	mv, ok := metrics[m.identifier]
	lock.RUnlock()
	if !ok {
		return fmt.Errorf("unknown collector: %v", m.identifier)
	}
	switch m.Type {
	case "Gauge":
		v, ok := mv.(*prometheus.GaugeVec)
		if !ok {
			return fmt.Errorf(errUnexpectedType, m.Type, mv)
		}
		c, err := v.GetMetricWithLabelValues(m.LabelValues...)
		if err != nil {
			return err
		}
		switch m.Method {
		case "Set":
			c.Set(m.Value)
		case "Inc":
			c.Inc()
		case "Dec":
			c.Dec()
		case "Add":
			c.Add(m.Value)
		case "Sub":
			c.Sub(m.Value)
		default:
			return fmt.Errorf(errUnknownMethod, m.Method, m.Type)
		}
	case "Untyped":
		v, ok := mv.(*prometheus.UntypedVec)
		if !ok {
			return fmt.Errorf(errUnexpectedType, m.Type, mv)
		}
		c, err := v.GetMetricWithLabelValues(m.LabelValues...)
		if err != nil {
			return err
		}
		switch m.Method {
		case "Set":
			c.Set(m.Value)
		case "Inc":
			c.Inc()
		case "Dec":
			c.Dec()
		case "Add":
			c.Add(m.Value)
		case "Sub":
			c.Sub(m.Value)
		default:
			return fmt.Errorf(errUnknownMethod, m.Method, m.Type)
		}
	case "Counter":
		v, ok := mv.(*prometheus.CounterVec)
		if !ok {
			return fmt.Errorf(errUnexpectedType, m.Type, mv)
		}
		c, err := v.GetMetricWithLabelValues(m.LabelValues...)
		if err != nil {
			return err
		}
		switch m.Method {
		case "Set":
			c.Set(m.Value)
		case "Inc":
			c.Inc()
		case "Add":
			c.Add(m.Value)
		default:
			return fmt.Errorf(errUnknownMethod, m.Method, m.Type)
		}
	case "Summary":
		v, ok := mv.(*prometheus.SummaryVec)
		if !ok {
			return fmt.Errorf(errUnexpectedType, m.Type, mv)
		}
		c, err := v.GetMetricWithLabelValues(m.LabelValues...)
		if err != nil {
			return err
		}
		switch m.Method {
		case "Observe":
			c.Observe(m.Value)
		default:
			return fmt.Errorf(errUnknownMethod, m.Method, m.Type)
		}
	case "Histogram":
		v, ok := mv.(*prometheus.HistogramVec)
		if !ok {
			return fmt.Errorf(errUnexpectedType, m.Type, mv)
		}
		c, err := v.GetMetricWithLabelValues(m.LabelValues...)
		if err != nil {
			return err
		}
		switch m.Method {
		case "Observe":
			c.Observe(m.Value)
		default:
			return fmt.Errorf(errUnknownMethod, m.Method, m.Type)
		}
	default:
		return fmt.Errorf("unknown type: %s", m.Type)
	}
	return nil
}

// MultiError is returned by batch operations when there are errors with
// particular elements. Errors will be in a one-to-one correspondence with
// the input elements; successful elements will have a nil entry.
type MultiError []error

func (m MultiError) Error() string {
	var strs []string
	for i, err := range m {
		if err != nil {
			strs = append(strs, fmt.Sprintf("[%d] %v", i, err))
		}
	}
	return strings.Join(strs, " ")
}

func (m MultiError) AsError() error {
	for _, e := range m {
		if e != nil {
			return m
		}
	}
	return nil
}
