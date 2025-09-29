package postgres

import (
	"Lora_Esp_Gsm_Gps_project/configs"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"Lora_Esp_Gsm_Gps_project/internal/models"
)

var ErrNotFound = errors.New("not found")

type Repo struct {
	db *sql.DB
}

func NewPostgresRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func NewPostgresDB(config configs.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		config.DbHost, config.DbPort, config.DbUser, config.DbPassword, config.DbName)

	var err error
	var db *sql.DB
	for i := 0; i < 5; i++ {
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			log.Printf("Failed to open database (attempt %d): %v", i+1, err)
			time.Sleep(2 * time.Second)
			continue
		}

		err = db.Ping()
		if err != nil {
			log.Printf("Failed to ping database (attempt %d): %v", i+1, err)
			db.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database after 5 attempts: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(10 * time.Minute)

	log.Println("Successfully connected to PostgreSQL!")
	return db, nil
}

func (r *Repo) InitTables() error {
	return r.CreatePacketsTable()
}

func (r *Repo) CreatePacketsTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS packets (
			request_id INTEGER PRIMARY KEY,
			rssi INTEGER NOT NULL,
			snrl DOUBLE PRECISION NOT NULL,
			latitude INTEGER NOT NULL,
			longitude INTEGER NOT NULL,
			hdop DOUBLE PRECISION NOT NULL,
			timestamp VARCHAR(55) NOT NULL
		)
	`)
	return err
}

func (r *Repo) Save(packet *models.Packet) error {
	_, err := r.db.Exec("INSERT INTO PACKETS "+
		"INSERT (request_id, rssi, snrl, latitude, longitude, hdop, timestamp) values ($1, $2, $3, $4, $5, $6, $7)",
		packet.RequestId,
		packet.RSSI,
		packet.SNRL,
		packet.Coordinate.Latitude,
		packet.Coordinate.Longitude,
		packet.Hdop,
		packet.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to save packet: %w", err)
	}
	return err
}

func (r *Repo) FindById(requestId int32) ([]models.Packet, error) {
	rows, err := r.db.Query(
		"SELECT "+
			"request_id, rssi, snrl, latitude, longitude, hdop, timestamp FROM packets WHERE request_id = $1",
		requestId,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []models.Packet{}, ErrNotFound
		}
	}
	defer rows.Close()
	var packets []models.Packet
	for rows.Next() {
		var packet models.Packet
		err := rows.Scan(
			&packet.RequestId,
			&packet.RSSI,
			&packet.SNRL,
			&packet.Coordinate.Latitude,
			&packet.Coordinate.Longitude,
			&packet.Hdop,
			&packet.Timestamp,
		)
		if err != nil {
			return nil, err
		}
		packets = append(packets, packet)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(packets) == 0 {
		return nil, ErrNotFound
	}

	return packets, nil
}
