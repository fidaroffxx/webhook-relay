package db

import (
	"database/sql"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

type DB struct {
	*sql.DB
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
		logrus.Fatal(err)
	}

	db := sql.OpenDB(c)

	err = db.Ping()
	if err != nil {
		logrus.Fatalf("%v - ошибка при подключении к базе", err)
	}

	logrus.Infof(
		"Connected to PostgreSQL at %s:%d db=%s user=%s",
		config.GetHost(),
		config.GetPort(),
		config.GetName(),
		config.GetUser(),
	)

	return &DB{
		DB: db,
	}
}
