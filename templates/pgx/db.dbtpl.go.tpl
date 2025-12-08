{{ define "db" }}
// DB is the common interface for database operations using pgx.
type DB interface {
        Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
        Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
        QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Scannable is an interface for types that can be scanned.
type Scannable interface {
        Scan(dest ...any) error
}

// Error is an error wrapper.
type Error struct {
        Err error
}

func (e *Error) Error() string {
        return e.Err.Error()
}

func (e *Error) Unwrap() error {
        return e.Err
}

// logerror logs an error and returns it wrapped.
func logerror(err error) error {
        if err == nil {
                return nil
        }
        if errorf != nil {
                errorf("error: %v", err)
        }
        return &Error{Err: err}
}

// logf logs a message if a logger is set.
func logf(s string, v ...any) {
        if logfunc != nil {
                logfunc(s, v...)
        }
}

var (
        logfunc func(string, ...any)
        errorf  func(string, ...any)
)

// SetLogger sets the logger for the package.
func SetLogger(f func(string, ...any)) {
        logfunc = f
}

// SetErrorLogger sets the error logger for the package.
func SetErrorLogger(f func(string, ...any)) {
        errorf = f
}

// ErrNoRows is the pgx equivalent of sql.ErrNoRows.
var ErrNoRows = pgx.ErrNoRows

// Collect is a helper function to collect rows into a slice.
func Collect[T any](rows pgx.Rows, scanFn func(pgx.Rows) (T, error)) ([]*T, error) {
        defer rows.Close()
        var result []*T
        for rows.Next() {
                item, err := scanFn(rows)
                if err != nil {
                        return nil, logerror(err)
                }
                result = append(result, &item)
        }
        if err := rows.Err(); err != nil {
                        return nil, logerror(err)
        }
        return result, nil
}
{{ end }}
