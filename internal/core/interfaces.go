package core

import (
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
