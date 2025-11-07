package esp

type Connector interface {
	IsConnected() bool
	SendCommand(command string, sessionId int) error
}

var _ Connector = (*ESPConnector)(nil)
