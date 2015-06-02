package instrumental

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/rcrowley/go-metrics"
)

const DefaultAddr = "collector.instrumentalapp.com:8000"

var clientHello string

func init() {
	h, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	clientHello = fmt.Sprintf(
		"hello version go/alytvynov-instrumental/0.1.0 hostname %s pid %d runtime %s platform %s-%s\n",
		h,
		os.Getpid(),
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
	)
}

type Config struct {
	Addr   string
	Token  string
	Prefix string
}

func Instrumental(r metrics.Registry, d time.Duration, config Config) {
	if config.Addr == "" {
		config.Addr = DefaultAddr
	}
	for {
		connectAndSend(r, d, config)
		time.Sleep(time.Second)
	}
}

func connectAndSend(r metrics.Registry, d time.Duration, config Config) {
	con, err := net.Dial("tcp", config.Addr)
	if err != nil {
		log.Println("instrumental:", err)
		return
	}
	defer con.Close()

	if err = setup(con, config); err != nil {
		log.Println("instrumental:", err)
		return
	}

	go io.Copy(os.Stderr, con)

	for range time.Tick(d) {
		if err := send(r, con, config.Prefix); err != nil {
			log.Println("instrumental:", err)
		}
	}
}

func setup(con net.Conn, config Config) error {
	s := bufio.NewScanner(con)

	if _, err := con.Write([]byte(clientHello)); err != nil {
		return err
	}
	if !s.Scan() {
		return fmt.Errorf("no response for HELLO; error: %v", s.Err())
	}
	if s.Text() != "ok" {
		return fmt.Errorf("unsuccessful HELLO: %v", s.Text())
	}

	if _, err := con.Write([]byte(fmt.Sprintf("authenticate %s\n", config.Token))); err != nil {
		return err
	}
	if !s.Scan() {
		return fmt.Errorf("no response for AUTHENTICATE; error: %v", s.Err())
	}
	if s.Text() != "ok" {
		return fmt.Errorf("unsuccessful AUTHENTICATE: %v", s.Text())
	}

	return s.Err()
}

func send(r metrics.Registry, con net.Conn, prefix string) error {
	vals := make(map[string]float64)
	now := time.Now().Unix()
	r.Each(func(name string, i interface{}) {
		switch m := i.(type) {
		case metrics.Counter:
			vals[name] = float64(m.Count())
		case metrics.Gauge:
			vals[name] = float64(m.Value())
		case metrics.GaugeFloat64:
			vals[name] = m.Value()
		case metrics.Histogram:
			vals[name] = float64(m.Count())
		case metrics.Meter:
			vals[name] = float64(m.Count())
		case metrics.Timer:
			vals[name] = float64(m.Count())
		}
	})

	for n, v := range vals {
		if n[0] == '.' {
			n = n[1:]
		}
		if _, err := con.Write([]byte(fmt.Sprintf("gauge %s.%s %f %d\n", prefix, n, v, now))); err != nil {
			return err
		}
	}
	return nil
}
