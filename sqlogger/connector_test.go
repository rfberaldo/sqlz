package sqlogger

import (
	"database/sql/driver"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnector_Connect(t *testing.T) {
	t.Run("Connect Success", func(t *testing.T) {
		mockDriver := &driverMock{}
		mockDriver.On("Open", mock.Anything).Return(&connMock{}, nil)

		conn := &connector{"sqlite3", mockDriver, tLogger}
		_, err := conn.Connect(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "Connect", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.NotEmpty(t, output.data.ConnId)
	})

	t.Run("Connect Error", func(t *testing.T) {
		mockDriver := &driverMock{}
		mockDriver.On("Open", mock.Anything).Return(&connMock{}, driver.ErrBadConn)

		conn := &connector{"test", mockDriver, tLogger}
		_, err := conn.Connect(ctx)
		assert.Error(t, err)
		assert.Equal(t, "Connect", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.NotEmpty(t, output.data.ConnId)
	})
}

func TestConnector_Driver(t *testing.T) {
	mockDriver := &driverMock{}
	conn := &connector{"test", mockDriver, tLogger}
	assert.Equal(t, mockDriver, conn.Driver())
}
