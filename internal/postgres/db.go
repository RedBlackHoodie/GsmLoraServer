package postgres

import (
	"database/sql"
	"errors"
	"log/slog"
	"time"

	""
)

type Repo struct {
	db *sql.DB
}

func NewPostgresRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func (r *Repo) Save() error {
	_, err := r.db.Exec("INSERT INTO links (short, original, created_at) VALUES ($1, $2, $3) ON CONFLICT (short) DO NOTHING", shortURL, originalURL, time.Now())
	return err
}

func (r *Repo) Get(shortURL string, logger *slog.Logger) (string, error) {
	var originalURL string
	err := r.db.QueryRow("SELECT original FROM links WHERE short = $1", shortURL).Scan(&originalURL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return originalURL, nil
}
