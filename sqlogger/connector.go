package sqlogger

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

// connector implements [driver.Connector]
type connector struct {
	dsn    string
	driver driver.Driver
	logger *sqlogger
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	start := time.Now()
	lvl := slog.LevelDebug
	id := c.logger.idGenerator()

	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Connect", start, err, slog.String(connKey, id))

	return &connection{conn, id, c.logger}, err
}

func (c *connector) Driver() driver.Driver { return c.driver }
