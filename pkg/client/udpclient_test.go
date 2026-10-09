package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// nilConnDialer leaves c.conn nil after initConn succeeds: the state the
// exchange goroutine sees when Close takes c.lock between initConn and it.
type nilConnDialer struct{}

func (nilConnDialer) Dial(string, string) (net.Conn, error) { return nil, nil }

func TestUDPClientExchangeWithClosedConn(t *testing.T) {
	never := func([]byte) (bool, error) { return false, nil }
	exchanges := map[string]func(*UDPClient) error{
		"Exchange": func(c *UDPClient) error {
			_, err := c.Exchange(context.Background(), bytes.NewReader([]byte{0}))
			return err
		},
		"ExchangeUntilMatch": func(c *UDPClient) error {
			_, err := c.ExchangeUntilMatch(context.Background(), bytes.NewReader([]byte{0}), never)
			return err
		},
	}
	for name, exchange := range exchanges {
		t.Run(name, func(t *testing.T) {
			c := NewUDPClient("127.0.0.1", 623).SetProxy(nilConnDialer{})
			if err := exchange(c); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("got err %v, want net.ErrClosed", err)
			}
		})
	}
}

// Races a real Close against real exchanges over DialUDP, as closeLAN races
// keepSessionAlive in production. The window is narrow, so it loops.
func TestUDPClientExchangeRacingClose(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port

	never := func([]byte) (bool, error) { return false, nil }
	exchanges := map[string]func(*UDPClient) error{
		"Exchange": func(c *UDPClient) error {
			_, err := c.Exchange(context.Background(), bytes.NewReader([]byte{0}))
			return err
		},
		"ExchangeUntilMatch": func(c *UDPClient) error {
			_, err := c.ExchangeUntilMatch(context.Background(), bytes.NewReader([]byte{0}), never)
			return err
		},
	}

	const workers, iterations = 8, 5000
	for name, exchange := range exchanges {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range iterations {
						c := NewUDPClient("127.0.0.1", port).SetTimeout(time.Millisecond)
						done := make(chan struct{})
						go func() {
							defer close(done)
							_ = exchange(c)
						}()
						_ = c.Close()
						<-done
					}
				}()
			}
			wg.Wait()
		})
	}
}
