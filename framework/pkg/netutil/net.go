package netutil

import (
	"fmt"
	"net"
	"strconv"
)

func SplitHostPort(address string) (string, int, error) {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid rpc address %q: %w", address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port <= 0 {
		return "", 0, fmt.Errorf("invalid rpc address port %q", rawPort)
	}
	return host, port, nil
}
