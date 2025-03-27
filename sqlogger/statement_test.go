package sqlogger

import (
	"database/sql/driver"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestStatement_Close(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Close").Return(nil)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		err := stmt.Close()

		fmt.Printf("%+v", output.data)

		assert.NoError(t, err)
		assert.Equal(t, "StmtClose", output.data.Msg)
		assert.Equal(t, "", output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, stmt.connId, output.data.ConnId)
		assert.Equal(t, stmt.id, output.data.StmtId)
	})

	t.Run("Error", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Close").Return(driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		err := stmt.Close()
		assert.Error(t, err)
		assert.Equal(t, "StmtClose", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, stmt.connId, output.data.ConnId)
		assert.Equal(t, stmt.id, output.data.StmtId)
	})
}

func TestStatement_NumInput(t *testing.T) {
	q := "SELECT * FROM tt WHERE id = ?"
	stmtMock := &statementMock{}
	stmtMock.On("NumInput").Return(1)

	stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
	input := stmt.NumInput()
	assert.Equal(t, 1, input)
}

func TestStatement_Exec(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Exec", mock.Anything).Return(&resultMock{}, nil)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.Exec([]driver.Value{"testid"})
		assert.NoError(t, err)
		assert.Equal(t, "StmtExec", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Error", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Exec", mock.Anything).Return(driver.ResultNoRows, driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.Exec([]driver.Value{"testid"})
		assert.Error(t, err)
		assert.Equal(t, "StmtExec", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})
}

func TestStatement_Query(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Query", mock.Anything).Return(&rowsMock{}, nil)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.Query([]driver.Value{"testid"})
		assert.NoError(t, err)
		assert.Equal(t, "StmtQuery", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Error", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("Query", mock.Anything).Return(&rowsMock{}, driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.Query([]driver.Value{"testid"})
		assert.Error(t, err)
		assert.Equal(t, "StmtQuery", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})
}

func TestStatement_ExecContext(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("ExecContext", mock.Anything, mock.Anything).Return(&resultMock{}, nil)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.ExecContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.NoError(t, err)
		assert.Equal(t, "StmtExecContext", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Error", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("ExecContext", mock.Anything, mock.Anything).Return(&resultMock{}, driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.ExecContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrBadConn, err)
		assert.Equal(t, "StmtExecContext", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Not implement driver.StmtExecContext", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &basicStatementMock{}
		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}

		_, err := stmt.ExecContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestStatement_QueryContext(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("QueryContext", mock.Anything, mock.Anything).Return(&rowsMock{}, nil)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.QueryContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.NoError(t, err)
		assert.Equal(t, "StmtQueryContext", output.data.Msg)
		assert.Equal(t, slog.LevelInfo, output.data.Level)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Error", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &statementMock{}
		stmtMock.On("QueryContext", mock.Anything, mock.Anything).Return(&rowsMock{}, driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}
		_, err := stmt.QueryContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrBadConn, err)
		assert.Equal(t, "StmtQueryContext", output.data.Msg)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, driver.ErrBadConn.Error(), output.data.Error)
		assert.Equal(t, q, output.data.Query)
		assert.Equal(t, []any{"testid"}, output.data.Args)
	})

	t.Run("Not implement driver.StmtQueryContext", func(t *testing.T) {
		q := "SELECT * FROM tt WHERE id = ?"
		stmtMock := &basicStatementMock{}
		stmt := &statement{stmtMock, randomId(), randomId(), q, tLogger}

		_, err := stmt.QueryContext(ctx, []driver.NamedValue{{Name: "", Ordinal: 0, Value: "testid"}})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestStatement_ConnIdFlow(t *testing.T) {
	connMock := &connMock{}

	stmtMock := &statementMock{}
	stmtMock.On("Query", mock.Anything).Return(&rowsMock{}, nil)
	connMock.On("Prepare", mock.Anything).Return(stmtMock, nil)
	conn := &connection{connMock, randomId(), tLogger}

	q := "SELECT * FROM tt WHERE id = ?"
	stmt, err := conn.Prepare(q)
	assert.NoError(t, err)
	assert.Equal(t, slog.LevelDebug, output.data.Level)
	assert.Equal(t, conn.id, output.data.ConnId)

	_, err = stmt.Query([]driver.Value{1})
	assert.NoError(t, err)
	assert.Equal(t, slog.LevelInfo, output.data.Level)
	assert.Equal(t, conn.id, output.data.ConnId)
	assert.NotEmpty(t, output.data.StmtId)
}

func TestStatement_CheckNamedValue(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		stmtMock := &statementMock{}
		stmtMock.On("CheckNamedValue", mock.Anything).Return(driver.ErrBadConn)

		stmt := &statement{stmtMock, randomId(), randomId(), "", tLogger}
		err := stmt.CheckNamedValue(&driver.NamedValue{Name: "", Ordinal: 0, Value: "testid"})
		assert.Error(t, err)
		assert.Equal(t, slog.LevelError, output.data.Level)
		assert.Equal(t, "StmtCheckNamedValue", output.data.Msg)
		assert.NotEmpty(t, output.data.StmtId)
		assert.NotEmpty(t, output.data.ConnId)
	})

	t.Run("Not implement driver.NamedValueChecker", func(t *testing.T) {
		stmtMock := &basicStatementMock{}

		stmt := &statement{stmtMock, randomId(), randomId(), "", tLogger}
		err := stmt.CheckNamedValue(&driver.NamedValue{Name: "", Ordinal: 0, Value: "testid"})
		assert.Error(t, err)
		assert.Equal(t, driver.ErrSkip, err)
	})
}

func TestStatement_ColumnConverter(t *testing.T) {
	t.Run("Return as is", func(t *testing.T) {
		stmtMock := &statementMock{}
		stmtMock.On("ColumnConverter", mock.Anything).Return(driver.NotNull{Converter: driver.DefaultParameterConverter})

		stmt := &statement{stmtMock, randomId(), randomId(), "", tLogger}
		cnv := stmt.ColumnConverter(1)
		val, err := cnv.ConvertValue(1)
		assert.NoError(t, err)
		intVal, ok := val.(int64)
		assert.True(t, ok)
		assert.Equal(t, int64(1), intVal)
	})

	t.Run("Not implement driver.ColumnConverter", func(t *testing.T) {
		stmtMock := &basicStatementMock{}
		stmt := &statement{stmtMock, randomId(), randomId(), "", tLogger}
		cnv := stmt.ColumnConverter(1)
		assert.Equal(t, driver.DefaultParameterConverter, cnv)
	})
}
