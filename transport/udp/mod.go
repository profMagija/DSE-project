package udp

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	transport.Transport
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {

	addr, err := parseAddr(address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	soc := &Socket{
		running: true,
		address: conn.LocalAddr().(*net.UDPAddr),
		conn:    conn,

		ins:  nil,
		outs: nil,
	}

	return soc, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	transport.Socket
	transport.ClosableSocket

	running bool
	address *net.UDPAddr
	conn    *net.UDPConn

	inMutex sync.Mutex
	ins     []transport.Packet

	outMutex sync.Mutex
	outs     []transport.Packet
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	if !s.running {
		return net.ErrClosed
	}
	s.running = false
	s.conn.Close()

	return nil
}

func parseAddr(addr string) (*net.UDPAddr, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return nil, &net.AddrError{Err: "Invalid address", Addr: addr}
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(parts[0]),
	}, nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	data, err := pkt.Marshal()
	if err != nil {
		return err
	}

	addr, err := parseAddr(dest)
	if err != nil {
		return err
	}

	err = s.conn.SetWriteDeadline(getTimeOfDeadline(timeout))
	if err != nil {
		return err
	}
	_, err = s.conn.WriteToUDP(data, addr)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return transport.TimeoutErr(timeout)
	} else if err != nil {
		return err
	}

	s.outMutex.Lock()
	defer s.outMutex.Unlock()
	s.outs = append(s.outs, pkt)

	return nil
}

var MAX_TIME time.Time = time.Unix(0xfffffffffffffff, 0)

func getTimeOfDeadline(timeout time.Duration) time.Time {
	if timeout == 0 {
		return MAX_TIME
	} else {
		return time.Now().Add(timeout)
	}
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	var pkt transport.Packet
	buf := make([]byte, bufSize)

	err := s.conn.SetReadDeadline(getTimeOfDeadline(timeout))
	if err != nil {
		return pkt, err
	}
	length, _, err := s.conn.ReadFromUDP(buf)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return pkt, transport.TimeoutErr(timeout)
	} else if err != nil {
		return pkt, err
	}

	err = pkt.Unmarshal(buf[:length])
	if err != nil {
		return pkt, err
	}

	s.inMutex.Lock()
	defer s.inMutex.Unlock()
	s.ins = append(s.ins, pkt)

	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.address.String()
}

func getHelper(m *sync.Mutex, src *[]transport.Packet) []transport.Packet {
	m.Lock()
	defer m.Unlock()

	if *src == nil {
		return nil
	}

	dst := make([]transport.Packet, len(*src))
	copy(dst, *src)
	return dst
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return getHelper(&s.inMutex, &s.ins)
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return getHelper(&s.outMutex, &s.outs)
}
