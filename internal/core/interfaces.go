package core

import (
	"Lora_Esp_Gsm_Gps_project/internal/esp"
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"context"
	"database/sql"
	"net"
)

type Repository interface {
	FindById(ctx context.Context, id int) (interface{}, error)
}

type Service interface {
	ProcessData(data interface{}) error
}

type App interface {
	GetDB() *sql.DB
	GetRepo() Repository
	GetService() Service
	Close() error
}

type MeasurementHandler interface {
	ProcessPacketData(msg string) error
	ProcessInterfaceSettingChange(conn net.Conn, msg string) error
	GetMeasurements(requestID int) ([]models.Packet, error)
	SendMeasurementCommand(conn net.Conn, command string, sessionId int) error
	SendMeasurementsToClient(conn net.Conn, command string) error
	GetAllSessions() ([]models.Session, error)
	SendAllSessions(conn net.Conn, data string) error
	SaveSession(session models.Session) error
	RemoveSession(sessionId int) error
	EspInitializer(espConnector esp.Connector) error
}
