package nsqout

import (
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/nsqio/go-nsq"
)

type publishFn func(
	keys outil.Selector,
	data []publisher.Event,
) ([]publisher.Event, error)

type client struct {
	outputs.NetworkClient

	stats outputs.Observer
	codec codec.Codec
	index string

	//for nsq
	nsqd     string
	topic    string
	producer *nsq.Producer
	config   *nsq.Config
}

func newClient(
	stats outputs.Observer,
	codec codec.Codec,
	index string,
	nsqd string,
	topic string,
	writeTimeout time.Duration,
	dialTimeout time.Duration,
) *client {
	cfg := nsq.NewConfig()
	cfg.WriteTimeout = writeTimeout
	cfg.DialTimeout = dialTimeout
	return &client{
		codec:  codec,
		stats:  stats,
		index:  index,
		nsqd:   nsqd,
		config: cfg,
		topic:  topic,
	}
}

func (c *client) Connect() error {
	debugf("connect to %s", c.nsqd)

	producer, err := nsq.NewProducer(c.nsqd, c.config)
	if err != nil {
		logp.Err("[main:NsqForward.Open] NewProducer error ", err)
		return err
	}
	//TODO: set logger
	//producer.SetLogger(nullLogger, LogLevelInfo)
	c.producer = producer
	return err
}

func (c *client) Publish(batch publisher.Batch) error {
	if c == nil {
		panic("no client")
	}
	if batch == nil {
		panic("no batch")
	}

	events := batch.Events()
	c.stats.NewBatch(len(events))

	st := c.stats

	//build message failed
	msgs, err := c.buildNsqMessages(events)
	dropped := len(events) - len(msgs)
	logp.Info("events=%v msgs=%v", len(events), len(msgs))
	if err != nil {
		//	st.Dropped(dropped)
		//	st.Acked(len(events) - dropped)
		logp.Err("[main:nsq] c.buildNsqMessages ", err)
		c.stats.Failed(len(events))
		batch.RetryEvents(events)
		return err
	}

	//nsq send failed do retry...
	err = c.producer.MultiPublish(c.topic, msgs)
	if err != nil {
		//logp.Err("[main:nsq] producer.MultiPublish ", err)
		c.stats.Failed(len(events))
		batch.RetryEvents(events)
		return err
	}
	batch.ACK()

	st.Dropped(dropped)
	st.Acked(len(msgs))
	return err
}

func (c *client) buildNsqMessages(events []publisher.Event) ([][]byte, error) {
	length := len(events)
	msgs := make([][]byte, length)
	var total sync.Map
	var count int
	maplog := make(map[string]interface{})
	var err error
	
	for idx := 0; idx < length; idx++ {
		event := events[idx].Content
		serializedEvent, nerr := c.codec.Encode(c.index, &event)
		//fmt.Printf("fmt.Event %p --- %p\n", &event, serializedEvent)
		if nerr != nil {
			logp.Err("[main:nsq] c.codec.Encode ", err)
			err = nerr
		} else {
			//should copy, see https://blog.golang.org/go-slices-usage-and-internals
			tmp := string(serializedEvent)
			data := strings.Split(tmp, "\t")
			time1 := data[0]
			loc, _ := time.LoadLocation("Local")
			times, _ := time.ParseInLocation("02/Jan/2006:15:04:05 +0800", time1, loc)
			timestamp := times.Unix()
			ts = timestamp - timestamp%300
			domain: = data[9]
			command: = data[5]
			bytes_received: = strconv.ParseInt(data[6], 10, 64)
			bytes_sent: = strconv.ParseInt(data[7], 10, 64)
			key:=domain+" "+ts+" "+command
			vv, _ := total.Load(key)
            if vv != nil {
                vc, _ := vv.(string)
				value := strings.Split(filemap[key], "")
				rflow := strconv.Atoi(value[0])
				sflow := strconv.Atoi(value[1])
				rf := strconv.Itoa(rflow + body_received)
				sf := strconv.Itoa(sflow + bytes_sent)
                total.Delete(key)
                total.Store(key,rf + " " + sf)
            } else {
                total.Store(key,strconv.Itoa(bytes_received) + " " + strconv.Itoa(bytes_sent)
            }
			//logp.Info("main:nsq] BuildMessage: %v", string(serializedEvent))
		}
	}
	total.Range(func(k, v interface{}) bool {
		sk := strings.Split(k.(string), " ")
        timestamp, _ := strconv.ParseInt(sk[1], 10, 64)
        maplog["domain"] = sk[0]
        maplog["timestamp"] = timestamp
        maplog["command"] = sk[2]
       	vs = v.(string)
		value := strings.Split(vs, "")
		rflow := strconv.Atoi(value[0])
		sflow := strconv.Atoi(value[1])
		maplog["bytes_received"]=rflow
		maplog["bytes_send"]=sflow
        total.Delete(k)
        jsonstr, err := json.Marshal(maplog)
        if err != nil {
             panic(err)
        }
        if jsonstr == nil {
			logmsg:=string(jsonstr)
			msgs[count] = []byte(logmsg)	        
			count++
		}

	return msgs[:count], err
}

