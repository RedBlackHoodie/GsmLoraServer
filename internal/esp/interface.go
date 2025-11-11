package esp

import "net"

type Connector interface {
	IsConnected() bool
	SendCommand(command string, sessionId int) error
	GetIP() string
	GetPort() string
	GetConn() net.Conn
	SetIP(ip string)
	SetPort(port string)
	SetConn(conn net.Conn)
	SetConnected(connected bool)
	SetCurrentSession(session int)
}

var _ Connector = (*ESPConnector)(nil)
