# pgx Template for dbtpl

This template generates Go code that uses the `github.com/jackc/pgx/v5` PostgreSQL driver instead of the standard `database/sql` + `github.com/lib/pq` combination.

## Features

- Uses native pgx types (`pgx.Rows`, `pgx.Row`, `pgconn.CommandTag`)
- Uses `pgtype.*` types for nullable database columns
- Compatible with `pgx.Conn`, `pgxpool.Pool`, and `pgx.Tx`
- Context-aware by default (all operations require `context.Context`)
- Generates proper CRUD operations (Insert, Update, Upsert, Delete, Save)
- Supports enums, indexes, and foreign keys

## Type Mappings

| PostgreSQL Type | Non-Nullable Go Type | Nullable Go Type |
|-----------------|---------------------|------------------|
| boolean | bool | pgtype.Bool |
| smallint | int16 | pgtype.Int2 |
| integer | int | pgtype.Int4 |
| bigint | int64 | pgtype.Int8 |
| real | float32 | pgtype.Float4 |
| double precision | float64 | pgtype.Float8 |
| numeric | float64 | pgtype.Float8 |
| text, varchar, char | string | pgtype.Text |
| bytea | []byte | []byte |
| timestamp | time.Time | pgtype.Timestamp |
| timestamptz | time.Time | pgtype.Timestamptz |
| date | time.Time | pgtype.Date |
| uuid | pgtype.UUID | pgtype.UUID |
| json, jsonb | []byte | []byte |

## Usage

```sh
# Generate models for a PostgreSQL database using pgx
dbtpl schema -t pgx postgres://user:pass@localhost/dbname -o models

# With custom options
dbtpl schema -t pgx postgres://user:pass@localhost/dbname -o models \
    --pkg mymodels \
    --field-tag 'json:"{{ .SQLName }}" db:"{{ .SQLName }}"'
```

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-2, --not-first` | Disable package file (not first generated file) | false |
| `--int32` | int32 type | "int" |
| `--uint32` | uint32 type | "uint" |
| `--pkg` | Package name | directory name |
| `--tag` | Build tags | none |
| `--import` | Additional package imports | none |
| `--uuid` | UUID type package | "github.com/google/uuid" |
| `--custom` | Package name for custom types | none |
| `--conflict` | Name conflict suffix | "Val" |
| `--initialism` | Add initialism (e.g. ID, API, URI) | none |
| `--esc` | Escape fields (none, schema, table, column, all) | "none" |
| `-g, --field-tag` | Field tag template | `json:"{{ .SQLName }}" db:"{{ .SQLName }}"` |
| `--context` | Context mode (only, disable, both) | "only" |
| `--inject` | Insert code into generated file headers | none |
| `--inject-file` | Insert code from file into headers | none |

## Generated Code Example

```go
// User represents a row from 'public.users'.
type User struct {
    ID        int64            `json:"id" db:"id"`           // id
    Name      string           `json:"name" db:"name"`       // name
    Email     pgtype.Text      `json:"email" db:"email"`     // email (nullable)
    CreatedAt time.Time        `json:"created_at" db:"created_at"` // created_at
    // xo fields
    _exists bool
}

// Insert inserts the [User] to the database.
func (u *User) Insert(ctx context.Context, db DB) error {
    // ...
}

// UserByID retrieves a row from 'public.users' as a [User].
func UserByID(ctx context.Context, db DB, id int64) (*User, error) {
    // ...
}
```

## DB Interface

The generated code works with any type implementing the `DB` interface:

```go
type DB interface {
    Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
```

This is compatible with:
- `*pgx.Conn`
- `*pgxpool.Pool`
- `pgx.Tx`

## Integration

To add this template to dbtpl, update the `templates/templates.go` file's embed directive:

```go
//go:embed createdb
//go:embed dot
//go:embed go
//go:embed json
//go:embed yaml
//go:embed pgx  // ADD THIS LINE
var files embed.FS
```

## Differences from Standard Go Template

1. **Database Interface**: Uses pgx-native types instead of `database/sql`
2. **Type System**: Uses `pgtype.*` types for nullable columns instead of `sql.Null*` types
3. **Error Handling**: Uses `pgx.ErrNoRows` instead of `sql.ErrNoRows`
4. **Query Execution**: All queries require `context.Context` parameter (pgx design)
5. **Connection**: Works with pgx connection types, not `*sql.DB`
