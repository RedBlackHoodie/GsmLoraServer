package core

import (
	"Lora_Esp_Gsm_Gps_project/internal/models"
	"context"
	"database/sql"
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
	ProcessInterfaceSettingChange(msg string) error
	GetMeasurements(requestID int32) ([]models.Packet, error)
	SendMeasurementCommand(ip, port, command string, sessionId int) error
	SendMeasurementsToClient(ip, port, command string) error
	GetAllSessions() ([]models.Session, error)
	SendAllSessions(ip, port, data string) error
	SaveSession(session models.Session) error
	RemoveSession(sessionId int32) error
}
