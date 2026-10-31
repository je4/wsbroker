package rest

import (
	"net"
	"time"

	"emperror.dev/errors"
	"golang.org/x/net/ipv4"
)

// NTPForwarder represents a function that sends a raw NTP packet and returns the NTP server response packet.
type NTPForwarder func(data []byte) ([]byte, error)

// NewNTPConnection creates a forwarder function that transparently forwards raw NTP UDP packets to a remote NTP server.
func NewNTPConnection(
	host string,
	protocol string,
	port string,
	localAddress string,
	ttl int,
	timeout time.Duration,
) NTPForwarder {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if protocol == "" {
		protocol = "udp"
	}
	if port == "" {
		port = "123"
	}

	// If host contains port, extract it
	if h, p, err := net.SplitHostPort(host); err == nil {
		host = h
		if p != "" {
			port = p
		}
	}

	return func(data []byte) ([]byte, error) {
		if host == "" {
			return nil, errors.New("ntp server host is empty")
		}

		// Resolve the remote NTP server address.
		raddr, err := net.ResolveUDPAddr(protocol, net.JoinHostPort(host, port))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot resolve remote NTP address %s:%s", host, port)
		}

		var laddr *net.UDPAddr
		if localAddress != "" {
			laddr, err = net.ResolveUDPAddr(protocol, net.JoinHostPort(localAddress, "0"))
			if err != nil {
				return nil, errors.Wrapf(err, "cannot resolve local NTP address %s", localAddress)
			}
		}

		// Prepare UDP connection to remote server.
		con, err := net.DialUDP(protocol, laddr, raddr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to dial NTP UDP server")
		}
		defer con.Close()

		// Set TTL if requested
		if ttl != 0 {
			ipcon := ipv4.NewConn(con)
			if err := ipcon.SetTTL(ttl); err != nil {
				return nil, errors.Wrapf(err, "failed to set TTL %d", ttl)
			}
		}

		if err := con.SetDeadline(time.Now().Add(timeout)); err != nil {
			return nil, errors.Wrap(err, "failed to set connection deadline")
		}

		// Transmit the query
		if _, err := con.Write(data); err != nil {
			return nil, errors.Wrap(err, "failed to transmit NTP query packet")
		}

		// Receive response (standard NTP packet is at least 48 bytes)
		bufSize := max(len(data), 48)
		recvMsg := make([]byte, bufSize)
		n, err := con.Read(recvMsg)
		if err != nil {
			return nil, errors.Wrap(err, "failed to receive NTP response packet")
		}

		return recvMsg[:n], nil
	}
}
