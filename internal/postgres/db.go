package postgres

import (
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"Lora_Esp_Gsm_Gps_project/internal/models"
)

var ErrNotFound = errors.New("Not found")

type Repo struct {
	db *sql.DB
}

func NewPostgresRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func (r *Repo) Save(packet models.Packet) error {
	_, err := r.db.Exec("INSERT INTO PACKETS (request_id, rssi, snrl, snrg, timestamp) values ($1, $2, $3, $4)", packet.RequestId, packet.RSSI, packet.SNRL, packet.SNRG, time.Now())
	return err
}

func (r *Repo) Get(requestId int32, logger *slog.Logger) (models.Packet, error) {
	var packet models.Packet
	err := r.db.QueryRow("SELECT (request_id, rssi, snrl, snrg, timestamp) FROM PACKETS WHERE request_id = $1", requestId).Scan(&packet)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Packet{}, ErrNotFound
		}
		return models.Packet{}, err
	}
	return packet, nil
}
