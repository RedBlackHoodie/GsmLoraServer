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

func (r *Repo) InitTables() (error, error) {
	return r.CreateSessionsTable(), r.CreatePacketsTable()
}

func (r *Repo) CreatePacketsTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS packets (
			request_id SERIAL PRIMARY KEY,
			rssi INTEGER NOT NULL,
			snrl DOUBLE PRECISION NOT NULL,
			latitude INTEGER NOT NULL,
			longitude INTEGER NOT NULL,
			hdop DOUBLE PRECISION NOT NULL,
			timestamp VARCHAR(55) NOT NULL,
		    session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(`
        CREATE OR REPLACE FUNCTION update_session_count()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE sessions 
            SET count = count + 1 
            WHERE id = NEW.session_id;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    `)
	if err != nil {
		return fmt.Errorf("failed to create trigger function: %w", err)
	}

	_, err = r.db.Exec(`
        DROP TRIGGER IF EXISTS increment_session_count ON packets;
        CREATE TRIGGER increment_session_count
        AFTER INSERT ON packets
        FOR EACH ROW
        EXECUTE FUNCTION update_session_count();
    `)

	if err != nil {
		return fmt.Errorf("failed to create trigger: %w", err)
	}

	return nil
}

func (r *Repo) CreateSessionsTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			id BIGINT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP,
		    count INTEGER NOT NULL
		)`)
	if err != nil {
		return fmt.Errorf("failed to create sessions table: %w", err)
	}
	return nil
}

func (r *Repo) Save(packet *models.Packet, sessionId int) error {
	var id int
	var err error
	if sessionId == 0 {
		id, err = r.FindLastSessionId()
		if err != nil {
			return err
		}
	} else {
		id = sessionId
	}
	_, err = r.db.Exec("INSERT INTO PACKETS "+
		/*request_id,*/ "(rssi, snrl, latitude, longitude, hdop, timestamp, session_id) values ($1, $2, $3, $4, $5, $6, $7)",
		//packet.RequestId,
		packet.RSSI,
		packet.SNRL,
		packet.Coordinate.Latitude,
		packet.Coordinate.Longitude,
		packet.Hdop,
		packet.Timestamp,
		id,
	)
	if err != nil {
		return fmt.Errorf("failed to save packet: %w", err)
	}
	return err
}

func (r *Repo) SaveSession(session models.Session) error {
	_, err := r.db.Exec("INSERT INTO SESSIONS "+
		"(id, name, start_time, end_time, count) values ($1, $2, $3, $4, $5)",
		session.Id,
		session.Name,
		session.StartTime,
		session.EndTime,
		session.Count,
	)
	if err != nil {
		return fmt.Errorf("failed to save session: %w", err)
	}
	return err
}

func (r *Repo) RemoveSession(sessionId int) error {
	_, err := r.db.Exec("DELETE FROM sessions WHERE id = $1", sessionId)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	return nil
}

func (r *Repo) GetAllSessions() ([]models.Session, error) {
	rows, err := r.db.Query("SELECT id, name, start_time, end_time, count " +
		"FROM sessions")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []models.Session{}, ErrNotFound
		}
		return nil, fmt.Errorf("failed to query sessions: %w", err)
	}
	defer rows.Close()

	var sessions []models.Session
	for rows.Next() {
		var session models.Session
		err := rows.Scan(
			&session.Id,
			&session.Name,
			&session.StartTime,
			&session.EndTime,
			&session.Count,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan session: %w", err)
		}
		sessions = append(sessions, session)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over sessions: %w", err)
	}

	if len(sessions) == 0 {
		return nil, ErrNotFound
	}

	return sessions, nil
}

func (r *Repo) FindById(sessionId int) ([]models.Packet, error) {
	rows, err := r.db.Query(
		"SELECT "+
			"rssi, snrl, latitude, longitude, hdop, timestamp FROM packets WHERE session_id = $1",
		sessionId,
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

func (r *Repo) FindLastRequestId() (int, error) {
	var lastRequestId int
	err := r.db.QueryRow("SELECT COALESCE(MAX(request_id), 0)" +
		" FROM packets").Scan(&lastRequestId)
	if err != nil {
		return 0, fmt.Errorf("failed to get last request ID: %w", err)
	}
	return lastRequestId, nil
}

func (r *Repo) FindLastSessionId() (int, error) {
	var lastSessionId int
	err := r.db.QueryRow("SELECT COALESCE(MAX(id), 0)" +
		" FROM sessions").Scan(&lastSessionId)
	if err != nil {
		return 0, fmt.Errorf("failed to get last request ID: %w", err)
	}
	return lastSessionId, nil
}
