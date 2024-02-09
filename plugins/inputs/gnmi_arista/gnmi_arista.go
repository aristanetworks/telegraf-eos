package gnmi_arista

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aristanetworks/goarista/gnmi"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/inputs"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var sampleConfig = `
  ## The plugin supports multiple instances. The config (credentials,
  ## gnmi_config, tls_config etc) provided for a given instance will
  ## be used for all targets specified in that instance.

  ## Target addresses and credentials if required.
  # targets = ["localhost:6030"]
  # username = "admin"
  # password = ""

  ## Secure transport can be used if required.
  # enable_tls = false
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Retry on initial connection error (exponential backoff).
  # retry_on_dial_err = true

  ## Subscription options
  ## stream_mode        - Stream subscription mode can be "sample", "on_change"
  ##                      or "target_defined".
  ## updates_only       - send delta updates rather then actual values.
  ## sample_interval    - sampling interval when using sample stream_mode
  ## suppress_redundant - when using sample stream_mode send updates for only
  ##                      those leaf nodes that have changed.
  ## heartbeat_interval - interval for periodic heartbeat updates when using
  ##                      on_change stream_mode or when using sample stream_mode
  ##                      with suppress_redundant enabled.
  ## For a detailed explanation of these options, see:
  ## reference/rpc/gnmi/gnmi-specification.md#351-managing-subscriptions
  #
  # stream_mode = "sample"
  # updates_only = false
  # sample_interval = "1s"
  # suppress_redundant = false
  # heartbeat_interval = "1s"

  ## Config file containing the details of the paths to subscribe to.
  # config_file = "/etc/telegraf/gnmi.json"
  #
  ## It should be in JSON. A sample config looks as follows:
  ##
  ## {
  ##   "comment" : "<optional_comment_for_other_users>",
  ##   "origin"  : "<optional_path_origin>",
  ##   "prefix"  : "<optional_global_prefix>",
  ##   "subscriptions": [
  ##     "/interfaces/interface/state/counters",
  ##     "<subscription_path_2>"
  ##   ],
  ##   "metrics": {
  ##     "<measurement1>": {
  ##       "path": "<path_regex_use_to_filterout_relevant_data>"
  ##     },
  ##     "intfCounters": {
  ##       "path": "/interfaces/interface\\[name=(?P<intf>.+)\\]/state/counters/(.+)"
  ##     },
  ##     "<measurement3>": {
  ##       "path": "/component\\[name=(?P<tag>.+)\\]/property\\[name=(?P<fieldKey>.+)\\]/state/value"
  ##     },
  ##     "operStatus": {
  ##       "path":"/Sysdb/interface/status/eth/phy/slice/1/intfStatus/(?P<intf>.+)/operStatus",
  ##       "StaticValueMap":{
  ##         "intfOperUp": 1,
  ##         "intfOperDown": 0,
  ##         "intfOperNotPresent": 0,
  ##         "default": 0
  ##       }
  ##     }
  ##   }
  ## }
  ##
  ## "fieldKey" is an optional special regex keyword which can to extract the
  ## field name(key) from the path when the last element name is not appropriate.
  ##
  ## E.g. the path below has a useful last element name "out-multicast-pkts"
  ## /interfaces/interface[name=Ethernet1]/state/counters/out-multicast-pkts
  ##
  ## However, in the following path, "value" is not a good field key and we can
  ## use the special regex to extact "current".
  ## /components/component[name=CurrentSensor1]/properties/property[name=current]/state/value
`

type GNMIArista struct {
	Targets  []string `toml:"targets"`
	Username string   `toml:"username"`
	Password string   `toml:"password"`

	RetryOnDialErr bool `toml:"retry_on_dial_err"`

	EnableTLS bool   `toml:"enable_tls"`
	TLSCA     string `toml:"tls_ca"`
	TLSCert   string `toml:"tls_cert"`
	TLSKey    string `toml:"tls_key"`

	StreamMode        string            `toml:"stream_mode"`
	UpdatesOnly       bool              `toml:"updates_only"`
	SampleInterval    internal.Duration `toml:"sample_interval"`
	SuppressRedundant bool              `toml:"suppress_redundant"`
	HeartbeatInterval internal.Duration `toml:"heartbeat_interval"`

	ConfigFile string `toml:"config_file"`

	Config

	acc telegraf.Accumulator
	Log telegraf.Logger

	eg     errgroup.Group
	cancel context.CancelFunc
}

func (g *GNMIArista) Description() string {
	return "gNMI based telemetry plugin for Arista devices"
}

func (g *GNMIArista) SampleConfig() string {
	return sampleConfig
}

func (g *GNMIArista) Init() error {
	return nil
}

func (g *GNMIArista) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (g *GNMIArista) Stop() {
	g.cancel()
	g.eg.Wait()
}

func init() {
	inputs.Add("gnmi_arista", func() telegraf.Input {
		return &GNMIArista{
			Targets:           []string{"localhost:6030"},
			Username:          "admin",
			RetryOnDialErr:    true,
			StreamMode:        "sample",
			SampleInterval:    internal.Duration{Duration: 10 * time.Second},
			HeartbeatInterval: internal.Duration{Duration: 10 * time.Second},
			ConfigFile:        "/etc/telegraf/gnmi.json",
		}
	})
}

// Config is the representation of the JSON config file
type Config struct {
	// Origin, if set, can be used to disambiguate the path if required
	Origin string

	// Prefix, if set, is used to prefix all the subscription paths.
	Prefix string

	// Paths to subscribe to.
	Subscriptions []string

	// Metrics to collect and how to munge them.
	Metrics map[string]*Metric
}

// Metric is the representation of a given metric we are interested in.
type Metric struct {
	// Path is a regexp to match on the update's full path.
	// The regexp must be a prefix match.
	// The regexp can define named capture groups to use as tags.
	Path string

	// Path compiled as a regexp.
	re *regexp.Regexp

	// Additional tags to add to this metric.
	Tags map[string]string

	// Optional static value map
	StaticValueMap map[string]int64
}

func (g *GNMIArista) loadConfig() error {
	cfg, err := ioutil.ReadFile(g.ConfigFile)
	if err != nil {
		return fmt.Errorf("Failed to load config: %v", err)
	}
	err = json.Unmarshal(cfg, &g.Config)
	if err != nil {
		return fmt.Errorf("Failed to parse config: %v", err)
	}
	for _, metric := range g.Config.Metrics {
		metric.re = regexp.MustCompile(metric.Path)
	}
	return nil
}

// Match applies this config to the given OpenConfig path.
// If the path doesn't match anything in the config, an empty string
// is returned as the metric name.
func (c *Config) Match(metricPath string) (
	measurement string,
	fieldKey string,
	tags map[string]string,
	staticValueMap map[string]int64) {
	tags = make(map[string]string)
	staticValueMap = make(map[string]int64)

	for name, metric := range c.Metrics {
		found := metric.re.FindStringSubmatch(metricPath)
		if found == nil {
			continue
		}
		measurement = name
		for i, subname := range metric.re.SubexpNames() {
			if i == 0 {
				continue
			} else if subname == "" {
				continue
			} else if subname == "fieldKey" {
				fieldKey = found[i]
			} else {
				tags[subname] = found[i]
			}
		}
		if fieldKey == "" {
			fieldKey = path.Base(metricPath)
		}
		for tag, value := range metric.Tags {
			tags[tag] = value
		}
		for initName, newName := range metric.StaticValueMap {
			staticValueMap[initName] = newName
		}
		break
	}
	return
}

func (g *GNMIArista) Start(acc telegraf.Accumulator) error {
	g.acc = acc

	if err := g.loadConfig(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel

	for _, address := range g.Targets {
		go func(target string) {
			cfg := &gnmi.Config{
				Addr:     target,
				CAFile:   g.TLSCA,
				CertFile: g.TLSCert,
				KeyFile:  g.TLSKey,
				Username: g.Username,
				Password: g.Password,
				TLS:      g.EnableTLS,
				DialOptions: []grpc.DialOption{
					grpc.WithBlock(),
					grpc.FailOnNonTempDialError(!g.RetryOnDialErr),
				},
			}

			ctx = gnmi.NewContext(ctx, cfg)
			client, err := gnmi.DialContext(ctx, cfg)
			if err != nil {
				if ctx.Err() != context.Canceled {
					g.Log.Errorf("Error: dialing target %s: %s", target, err)
				}
				return
			}

			respChan := make(chan *pb.SubscribeResponse)
			subscribeOptions := &gnmi.SubscribeOptions{
				UpdatesOnly:       g.UpdatesOnly,
				Prefix:            g.Config.Prefix,
				Mode:              "stream",
				StreamMode:        g.StreamMode,
				SampleInterval:    uint64(g.SampleInterval.Duration.Nanoseconds()),
				SuppressRedundant: g.SuppressRedundant,
				HeartbeatInterval: uint64(g.HeartbeatInterval.Duration.Nanoseconds()),
				Paths:             gnmi.SplitPaths(g.Config.Subscriptions),
				Origin:            g.Config.Origin,
			}

			g.eg.Go(func() error {
				err := gnmi.SubscribeErr(ctx, client, subscribeOptions, respChan)
				if err != nil && ctx.Err() != context.Canceled {
					g.Log.Errorf("Error: subscribing to target %s: %v", target, err)
				}
				return nil
			})

			g.eg.Go(func() error {
				mg := metric.NewSeriesGrouper()
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case resp, open := <-respChan:
						g.handleResponse(target, mg, resp.GetUpdate())
						if !open {
							for _, metric := range mg.Metrics() {
								g.acc.AddMetric(metric)
							}
							return nil
						}
					case <-ticker.C:
						for _, metric := range mg.Metrics() {
							g.acc.AddMetric(metric)
						}
						mg = metric.NewSeriesGrouper()
					}
				}
			})
		}(address)
	}
	return nil
}

func (g *GNMIArista) handleResponse(target string, mg *metric.SeriesGrouper, notif *pb.Notification) {
	if notif == nil {
		return
	}

	if notif.Timestamp <= 0 {
		g.Log.Errorf("Invalid timestamp %d in notification %v", notif.Timestamp, notif)
		return
	}

	timestamp := time.Unix(0, notif.Timestamp)
	prefix := gnmi.StrPath(notif.Prefix)

	for _, update := range notif.Update {
		metricPath := prefix + gnmi.StrPath(update.Path)
		measurement, fieldKey, tags, staticValueMap := g.Config.Match(metricPath)
		if measurement == "" || fieldKey == "" {
			continue
		}

		val, err := gnmi.ExtractValue(update)
		if err != nil {
			g.Log.Errorf("Error: extracting value from update %v: %v", update, err)
			continue
		}
		value := g.parseValue(val, staticValueMap)
		if value == nil {
			continue
		}

		// Add a target tag if not monitoring localhost
		if !strings.Contains(target, "localhost") && !strings.HasPrefix(target, "unix") {
			tags["target"] = target
		}

		for i, v := range value {
			if len(value) > 1 {
				tags["index"] = strconv.Itoa(i)
			}
			mg.Add(measurement, tags, timestamp, fieldKey, v)
		}
	}
}

// parseValue returns either an integer/floating point value of the given update, or if
// the value is a slice of integers/floating point values. If the value is neither of these
// or if any element in the slice is non numerical, parseValue returns nil.
func (g *GNMIArista) parseValue(value interface{}, staticValueMap map[string]int64) []interface{} {
	switch value := value.(type) {

	case bool:
		if value {
			return []interface{}{1}
		} else {
			return []interface{}{0}
		}

	case int64, uint64, float32:
		return []interface{}{value}

	case []byte:
		// OpenConfig models are defined in YANG and YANG doesn't have a floating point
		// type (only a fixed point Decimal64 type). The OpenConfig group wanted a floating
		// point type, so they defined one as a byte array with length 4.
		val := math.Float32frombits(binary.BigEndian.Uint32(value))
		return []interface{}{val}

	case *pb.Decimal64:
		val := gnmi.DecimalToFloat(value)
		if math.IsInf(val, 0) || math.IsNaN(val) {
			return nil
		}
		return []interface{}{val}

	case json.Number:
		var val interface{}
		var err error
		if val, err = value.Int64(); err != nil {
			if strings.Contains(err.Error(), "value out of range") {
				if val, err = strconv.ParseUint(value.String(), 10, 64); err != nil {
					g.Log.Errorf("Error: decoding %v: %v", val, err)
					return nil
				}
			} else if val, err = value.Float64(); err != nil {
				g.Log.Errorf("Error: decoding %v: %v", val, err)
				return nil
			}
		}
		return []interface{}{val}

	case []interface{}:
		for i, val := range value {
			value[i] = g.parseValue(val, staticValueMap)[0]
		}
		return value

	case map[string]interface{}:
		// Special case for simple value types that just have a "value"
		// attribute (common case).
		if val, ok := value["value"].(json.Number); ok && len(value) == 1 {
			return g.parseValue(val, staticValueMap)
		}

	case string:
		if newval, ok := staticValueMap[value]; ok {
			return []interface{}{newval}
		} else if newval, ok := staticValueMap["default"]; ok {
			return []interface{}{newval}
		} else {
			return nil
		}

	default:
		g.Log.Errorf("Ignoring unrecognised value %v of type %T", value, value)
	}
	return nil
}
