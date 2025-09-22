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
	ProcessInterfaceRequest(msg string) error
	GetMeasurements(requestID int) ([]models.Packet, error)
}
