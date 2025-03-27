package sqlogger

import (
	"database/sql/driver"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnection_Begin(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		connMock := &connMock{}
		txMock := &transactionMock{}
		connMock.On("Begin").Return(txMock, nil)

		conn := &connection{connMock, randomId(), tLogger}
		tx, err := conn.Begin()
		assert.NoError(t, err)
		assert.Implements(t, (*driver.Tx)(nil), tx)
		assert.Equal(t, "Begin", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Error", func(t *testing.T) {
		connMock := &connMock{}
		var txMock *transactionMock
		connMock.On("Begin").Return(txMock, driver.ErrBadConn)

		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Begin()
		assert.Error(t, err)

		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
	})
}

func TestConnection_Prepare(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		connMock := &connMock{}
		stmtMock := &statementMock{}
		connMock.On("Prepare", mock.Anything).Return(stmtMock, nil)
		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		stmt, err := conn.Prepare(q)
		assert.NoError(t, err)
		assert.Implements(t, (*driver.Stmt)(nil), stmt)
		assert.Equal(t, "Prepare", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, conn.id, output.data.ConnId)
		assert.NotEmpty(t, output.data.StmtId)
	})

	t.Run("Error", func(t *testing.T) {
		connMock := &connMock{}
		var stmtMock *statementMock
		connMock.On("Prepare", mock.Anything).Return(stmtMock, driver.ErrBadConn)
		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Prepare(q)
		assert.Error(t, err)
		assert.Equal(t, "Prepare", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, conn.id, output.data.ConnId)
	})
}

func TestConnection_Close(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("Close").Return(nil)
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.Close()
		assert.NoError(t, err)
		assert.Equal(t, "Close", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Error", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("Close").Return(driver.ErrBadConn)
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.Close()
		assert.Error(t, err)
		assert.Equal(t, "Close", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, conn.id, output.data.ConnId)
	})
}

func TestConnection_BeginTx(t *testing.T) {
	t.Run("With driver.ConnBeginTx Success", func(t *testing.T) {
		connMock := &connMock{}
		txMock := &transactionMock{}
		connMock.On("BeginTx", mock.Anything, mock.Anything).Return(txMock, nil)

		conn := &connection{connMock, randomId(), tLogger}
		tx, err := conn.BeginTx(ctx, driver.TxOptions{
			Isolation: 1,
			ReadOnly:  true,
		})
		assert.NoError(t, err)
		assert.Implements(t, (*driver.Tx)(nil), tx)

		assert.Equal(t, "BeginTx", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
		assert.NotEmpty(t, output.data.TxId)
	})

	t.Run("With driver.ConnBeginTx Error", func(t *testing.T) {
		connMock := &connMock{}
		var txMock *transactionMock
		connMock.On("BeginTx", mock.Anything, mock.Anything).Return(txMock, driver.ErrBadConn)

		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.BeginTx(ctx, driver.TxOptions{
			Isolation: 1,
			ReadOnly:  true,
		})
		assert.Error(t, err)

		assert.Equal(t, "BeginTx", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
		assert.NotEmpty(t, output.data.TxId)
	})

	t.Run("Non driver.ConnBeginTx", func(t *testing.T) {
		connMock := &basicConnMock{}
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.BeginTx(ctx, driver.TxOptions{
			Isolation: 1,
			ReadOnly:  true,
		})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestConnection_PrepareContext(t *testing.T) {
	t.Run("With driver.ConnBeginTx Success", func(t *testing.T) {
		connMock := &connMock{}
		stmtMock := &statementMock{}
		connMock.On("PrepareContext", mock.Anything).Return(stmtMock, nil)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		stmt, err := conn.PrepareContext(ctx, q)
		assert.NoError(t, err)
		assert.Implements(t, (*driver.Stmt)(nil), stmt)
		assert.Equal(t, "PrepareContext", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, conn.id, output.data.ConnId)
		assert.NotEmpty(t, output.data.StmtId)
	})

	t.Run("With driver.ConnPrepareContext Error", func(t *testing.T) {
		connMock := &connMock{}
		var stmtMock *statementMock
		connMock.On("PrepareContext", mock.Anything).Return(stmtMock, driver.ErrBadConn)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.PrepareContext(ctx, q)
		assert.Error(t, err)

		assert.Equal(t, "PrepareContext", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, conn.id, output.data.ConnId)
		assert.NotEmpty(t, output.data.StmtId)
	})

	t.Run("Non driver.ConnPrepareContext", func(t *testing.T) {
		connMock := &basicConnMock{}

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.PrepareContext(ctx, q)
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestConnection_Ping(t *testing.T) {
	t.Run("driver.Pinger Success", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("Ping", mock.Anything).Return(nil)
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.Ping(ctx)
		assert.NoError(t, err)

		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("driver.Pinger With Error", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("Ping", mock.Anything).Return(driver.ErrBadConn)
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.Ping(ctx)
		assert.Error(t, err)

		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.Pinger", func(t *testing.T) {
		connMock := &basicConnMock{}
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.Ping(ctx)
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestConnection_Exec(t *testing.T) {
	t.Run("driver.Execer Success", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := driver.ResultNoRows
		connMock.On("Exec", mock.Anything, mock.Anything).Return(resultMock, nil)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Exec(q, []driver.Value{"testid"})
		assert.NoError(t, err)

		assert.Equal(t, "Exec", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("driver.Execer Return Error", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := driver.ResultNoRows
		connMock.On("Exec", mock.Anything, mock.Anything).Return(resultMock, driver.ErrBadConn)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Exec(q, []driver.Value{1})
		assert.Error(t, err)
		assert.Equal(t, any(driver.ErrBadConn), err)

		assert.Equal(t, "Exec", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.Execer Will Return Error", func(t *testing.T) {
		connMock := &basicConnMock{}

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		res, err := conn.Exec(q, []driver.Value{1})
		assert.Nil(t, res)
		assert.Error(t, err)
		assert.Equal(t, any(driver.ErrSkip), err)
	})
}

func TestConnection_ExecContext(t *testing.T) {
	t.Run("driver.ExecerContext Success", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := driver.ResultNoRows
		connMock.On("ExecContext", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, nil)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.ExecContext(ctx, q, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.NoError(t, err)

		assert.Equal(t, "ExecContext", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("driver.ExecerContext Return Error", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := driver.ResultNoRows
		connMock.On("ExecContext", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, driver.ErrBadConn)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.ExecContext(ctx, q, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)

		assert.Equal(t, "ExecContext", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.ExecerContext Return Error args", func(t *testing.T) {
		connMock := &basicConnMock{}
		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.ExecContext(ctx, q, []driver.NamedValue{
			{Name: "errrrr", Ordinal: 0, Value: 1},
		})
		assert.Error(t, err)
	})
}

func TestConnection_Query(t *testing.T) {
	t.Run("driver.Queryer Success", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := &rowsMock{}
		connMock.On("Query", mock.Anything, mock.Anything).Return(resultMock, nil)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Query(q, []driver.Value{"testid"})
		assert.NoError(t, err)

		assert.Equal(t, "Query", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("driver.Queryer Return Error", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := &rowsMock{}
		connMock.On("Query", mock.Anything, mock.Anything).Return(resultMock, driver.ErrBadConn)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.Query(q, []driver.Value{"testid"})
		assert.Error(t, err)
		assert.Equal(t, any(driver.ErrBadConn), err)

		assert.Equal(t, "Query", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.Queryer Will Return Error", func(t *testing.T) {
		connMock := &basicConnMock{}

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		res, err := conn.Query(q, []driver.Value{1})
		assert.Nil(t, res)
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestConnection_QueryContext(t *testing.T) {
	t.Run("driver.QueryerContext Success", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := &rowsMock{}
		connMock.On("QueryContext", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, nil)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.QueryContext(ctx, q, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.NoError(t, err)
		assert.Equal(t, "QueryContext", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("driver.QueryerContext Return Error", func(t *testing.T) {
		connMock := &connMock{}
		resultMock := &rowsMock{}
		connMock.On("QueryContext", mock.Anything, mock.Anything, mock.Anything).Return(resultMock, driver.ErrBadConn)

		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.QueryContext(ctx, q, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)
		assert.Equal(t, "QueryContext", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.QueryerContext Return Error args", func(t *testing.T) {
		connMock := &basicConnMock{}
		q := "SELECT * FROM tt WHERE id = ?"
		conn := &connection{connMock, randomId(), tLogger}
		_, err := conn.QueryContext(ctx, q, []driver.NamedValue{
			{Name: "errrrr", Ordinal: 0, Value: 1},
		})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestConnection_ResetSession(t *testing.T) {
	t.Run("driver.SessionResetter Return Error", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("ResetSession", mock.Anything).Return(driver.ErrBadConn)

		conn := &connection{connMock, randomId(), tLogger}
		err := conn.ResetSession(ctx)
		assert.Error(t, err)

		assert.Equal(t, "ResetSession", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.SessionResetter", func(t *testing.T) {
		connMock := &basicConnMock{}
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.ResetSession(ctx)
		assert.Error(t, err)
		assert.Error(t, driver.ErrSkip, err)
	})
}

func TestConnection_CheckNamedValue(t *testing.T) {
	t.Run("driver.NamedValueChecker Return Error", func(t *testing.T) {
		connMock := &connMock{}
		connMock.On("CheckNamedValue", mock.Anything).Return(driver.ErrBadConn)

		conn := &connection{connMock, randomId(), tLogger}
		err := conn.CheckNamedValue(&driver.NamedValue{
			Name:    "",
			Ordinal: 0,
			Value:   "testid",
		})
		assert.Error(t, err)

		assert.Equal(t, "CheckNamedValue", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.NotEmpty(t, output.data.ConnId)
		assert.Equal(t, conn.id, output.data.ConnId)
	})

	t.Run("Non driver.NamedValueChecker", func(t *testing.T) {
		connMock := &basicConnMock{}
		conn := &connection{connMock, randomId(), tLogger}
		err := conn.CheckNamedValue(&driver.NamedValue{
			Name:    "",
			Ordinal: 0,
			Value:   "testid",
		})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}
