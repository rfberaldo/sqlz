package sqlogger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"log"
	"log/slog"
	"time"

	"github.com/stretchr/testify/mock"
)

var (
	output   = &writerMock{}
	tSlogger = slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tLogger  = &sqlogger{tSlogger, randomId, false}
	ctx      = context.Background()
)

type logData struct {
	Time     time.Time     `json:"time"`
	Level    slog.Level    `json:"level"`
	Msg      string        `json:"msg"`
	ConnId   string        `json:"conn_id"`
	StmtId   string        `json:"stmt_id"`
	TxId     string        `json:"tx_id"`
	Error    string        `json:"error"`
	Query    string        `json:"query"`
	Args     []any         `json:"args"`
	Duration time.Duration `json:"duration"`
}

// writerMock implements [io.Writer]
type writerMock struct {
	data logData
}

func (t *writerMock) Write(p []byte) (n int, err error) {
	t.data = logData{}
	err = json.Unmarshal(p, &t.data)
	if err != nil {
		log.Fatal(err)
	}

	return n, err
}

// driverMock implements [driver.Driver]
type driverMock struct {
	mock.Mock
}

func (m *driverMock) Open(name string) (driver.Conn, error) {
	arg := m.Called(name)
	return arg.Get(0).(driver.Conn), arg.Error(1)
}

// transactionMock implements [driver.Tx]
type transactionMock struct {
	mock.Mock
}

func (m *transactionMock) Commit() error {
	return m.Called().Error(0)
}
func (m *transactionMock) Rollback() error {
	return m.Called().Error(0)
}

// statementMock implements
// [driver.Stmt]
// [driver.StmtExecContext]
// [driver.StmtQueryContext]
// [driver.NamedValueChecker]
// [driver.ColumnConverter]
type statementMock struct {
	mock.Mock
}

func (m *statementMock) Close() error {
	return m.Called().Error(0)
}
func (m *statementMock) NumInput() int {
	return m.Called().Int(0)
}
func (m *statementMock) Exec(args []driver.Value) (driver.Result, error) {
	arg := m.Called(args)
	return arg.Get(0).(driver.Result), arg.Error(1)
}
func (m *statementMock) Query(args []driver.Value) (driver.Rows, error) {
	arg := m.Called(args)
	return arg.Get(0).(driver.Rows), arg.Error(1)
}
func (m *statementMock) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	arg := m.Called(ctx, args)
	return arg.Get(0).(driver.Result), arg.Error(1)
}
func (m *statementMock) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	arg := m.Called(ctx, args)
	return arg.Get(0).(driver.Rows), arg.Error(1)
}
func (m *statementMock) CheckNamedValue(nm *driver.NamedValue) error {
	return m.Called().Error(0)
}
func (m *statementMock) ColumnConverter(idx int) driver.ValueConverter {
	return m.Called(idx).Get(0).(driver.ValueConverter)
}

// basicStatementMock implements [driver.Stmt]
type basicStatementMock struct {
	mock.Mock
}

func (m *basicStatementMock) Close() error {
	return m.Called().Error(0)
}
func (m *basicStatementMock) NumInput() int {
	return m.Called().Int(0)
}
func (m *basicStatementMock) Exec(args []driver.Value) (driver.Result, error) {
	arg := m.Called(args)
	return arg.Get(0).(driver.Result), arg.Error(1)
}
func (m *basicStatementMock) Query(args []driver.Value) (driver.Rows, error) {
	arg := m.Called(args)
	return arg.Get(0).(driver.Rows), arg.Error(1)
}

// resultMock implements [driver.Result]
type resultMock struct{}

func (r *resultMock) LastInsertId() (int64, error) { return 0, nil }
func (r *resultMock) RowsAffected() (int64, error) { return 0, nil }

// rowsMock implements [driver.Rows]
type rowsMock struct{}

func (r *rowsMock) Columns() []string              { return []string{} }
func (r *rowsMock) Close() error                   { return nil }
func (r *rowsMock) Next(dest []driver.Value) error { return nil }

// connMock implements
// [driver.Conn]
// [driver.ConnBeginTx]
// [driver.ConnPrepareContext]
// [driver.Pinger]
// [driver.Execer]
// [driver.ExecerContext]
// [driver.Queryer]
// [driver.QueryerContext]
// [driver.SessionResetter]
// [driver.NamedValueChecker]
type connMock struct {
	mock.Mock
}

func (m *connMock) Prepare(query string) (driver.Stmt, error) {
	args := m.Called(query)
	return args.Get(0).(driver.Stmt), args.Error(1)
}
func (m *connMock) Close() error { return m.Called().Error(0) }
func (m *connMock) Begin() (driver.Tx, error) {
	return m.Called().Get(0).(driver.Tx), m.Called().Error(1)
}
func (m *connMock) Exec(query string, args []driver.Value) (driver.Result, error) {
	arg := m.Called(query, args)
	return arg.Get(0).(driver.Result), arg.Error(1)
}
func (m *connMock) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	arg := m.Called(ctx, query, args)
	return arg.Get(0).(driver.Result), arg.Error(1)
}
func (m *connMock) Query(query string, args []driver.Value) (driver.Rows, error) {
	arg := m.Called(query, args)
	return arg.Get(0).(driver.Rows), arg.Error(1)
}
func (m *connMock) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	arg := m.Called(ctx, query, args)
	return arg.Get(0).(driver.Rows), arg.Error(1)
}
func (m *connMock) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(driver.Tx), args.Error(1)
}
func (m *connMock) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	args := m.Called(query)
	return args.Get(0).(driver.Stmt), args.Error(1)
}
func (m *connMock) Ping(ctx context.Context) error { return m.Called().Error(0) }
func (m *connMock) ResetSession(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}
func (m *connMock) CheckNamedValue(nm *driver.NamedValue) error {
	return m.Called(nm).Error(0)
}

// basicConnMock implements [driver.Conn]
type basicConnMock struct {
	mock.Mock
}

func (m *basicConnMock) Prepare(query string) (driver.Stmt, error) {
	args := m.Called(query)
	return args.Get(0).(driver.Stmt), args.Error(1)
}
func (m *basicConnMock) Close() error { return m.Called().Error(0) }
func (m *basicConnMock) Begin() (driver.Tx, error) {
	return m.Called().Get(0).(driver.Tx), m.Called().Error(1)
}
