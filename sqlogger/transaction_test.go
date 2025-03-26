package sqlogger

import (
	"database/sql/driver"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction_Commit(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		txMock := &transactionMock{}
		txMock.On("Commit").Return(nil)

		tx := &transaction{txMock, randomId(), randomId(), tLogger}
		err := tx.Commit()
		assert.NoError(t, err)
		assert.Equal(t, "Commit", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, tx.id, output.data.TxId)
		assert.Equal(t, tx.connId, output.data.ConnId)
	})

	t.Run("Error", func(t *testing.T) {
		txMock := &transactionMock{}
		txMock.On("Commit").Return(driver.ErrBadConn)

		tx := &transaction{txMock, randomId(), randomId(), tLogger}
		err := tx.Commit()
		assert.Error(t, err)
		assert.Equal(t, "Commit", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, tx.id, output.data.TxId)
		assert.Equal(t, tx.connId, output.data.ConnId)
	})
}

func TestTransaction_Rollback(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		txMock := &transactionMock{}
		txMock.On("Rollback").Return(nil)

		tx := &transaction{txMock, randomId(), randomId(), tLogger}
		err := tx.Rollback()
		assert.NoError(t, err)
		assert.Equal(t, "Rollback", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, tx.id, output.data.TxId)
		assert.Equal(t, tx.connId, output.data.ConnId)
	})

	t.Run("Error", func(t *testing.T) {
		txMock := &transactionMock{}
		txMock.On("Rollback").Return(driver.ErrBadConn)

		tx := &transaction{txMock, randomId(), randomId(), tLogger}
		err := tx.Rollback()
		assert.Error(t, err)
		assert.Equal(t, "Rollback", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, tx.id, output.data.TxId)
		assert.Equal(t, tx.connId, output.data.ConnId)
	})
}
