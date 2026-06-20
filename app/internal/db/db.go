package db

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

type DB struct {
	DB *sql.DB
}

func NewDB(config *Config) *DB {
	cfg := pq.Config{
		Host:     config.GetHost(),
		Password: config.GetPassword(),
		Port:     config.GetPort(),
		User:     config.GetUser(),
		Database: config.GetName(),
		SSLMode:  "disable",
	}

	c, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(c)

	err = db.Ping()
	if err != nil {
		log.Fatalf("%v - ошибка при подключении к  базе", err)
	}

	return &DB{
		DB: db,
	}
}
