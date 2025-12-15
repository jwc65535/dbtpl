// Assumed schema for integration tests:
//   - authors(author_id serial PK, first_name text not null, last_name text not null, bio text null, created_at timestamptz not null)
//   - books(book_id serial PK, author_id int not null FK authors, title text not null, published_year int2 not null, isbn text unique not null, price numeric not null)
//   - tags(tag_id serial PK, tag_name text unique not null)
//   - book_tags(book_id int FK books, tag_id int FK tags, primary key(book_id, tag_id))
//   - advanced_features(feature_id serial PK, int_array int[], text_array text[], process_status text, point_location point,
//     int_range int4range, file_data bytea, email_address text)
//   - core_data_types table with common PostgreSQL types matching the CoreDataType struct fields.
package generated

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var testDB *pgx.Conn

type connAdapter struct {
	*pgx.Conn
}

func (c connAdapter) ExecContext(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return c.Exec(ctx, sql, arguments...)
}

func (c connAdapter) QueryContext(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.Query(ctx, sql, args...)
}

func (c connAdapter) QueryRowContext(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.QueryRow(ctx, sql, args...)
}

type txAdapter struct {
	pgx.Tx
}

func (t txAdapter) ExecContext(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return t.Exec(ctx, sql, arguments...)
}

func (t txAdapter) QueryContext(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Query(ctx, sql, args...)
}

func (t txAdapter) QueryRowContext(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.QueryRow(ctx, sql, args...)
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgresql://pgx:pgxpass@localhost:5432/pgx_test?sslmode=disable")
	if err != nil {
		os.Exit(1)
	}
	testDB = conn
	code := m.Run()
	defer func() {
		_ = testDB.Close(ctx)
	}()
	os.Exit(code)
}

func TestAuthorCRUD(t *testing.T) {
	ctx := context.Background()
	author := &Author{
		FirstName: "Jane",
		LastName:  "Doe",
		Bio:       pgtype.Text{String: "bio", Valid: true},
		CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	}
	adapter := connAdapter{testDB}
	if err := author.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert author: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.authors WHERE author_id=$1", author.AuthorID)

	fetched, err := AuthorByAuthorID(ctx, adapter, author.AuthorID)
	if err != nil {
		t.Fatalf("fetch author: %v", err)
	}
	if fetched.FirstName != author.FirstName || fetched.LastName != author.LastName {
		t.Fatalf("fetched author mismatch: %#v", fetched)
	}

	fetched.Bio = pgtype.Text{Valid: false}
	fetched.FirstName = "Janet"
	if err := fetched.Update(ctx, adapter); err != nil {
		t.Fatalf("update author: %v", err)
	}
	updated, err := AuthorByAuthorID(ctx, adapter, author.AuthorID)
	if err != nil {
		t.Fatalf("refetch author: %v", err)
	}
	if updated.FirstName != "Janet" || updated.Bio.Valid {
		t.Fatalf("update not persisted: %#v", updated)
	}

	missing, err := AuthorByAuthorID(ctx, adapter, -1)
	if err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected no rows for missing author, got %v %v", missing, err)
	}

	if err := fetched.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete author: %v", err)
	}
	_, err = AuthorByAuthorID(ctx, adapter, author.AuthorID)
	if err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected no rows after delete, got %v", err)
	}
}

func TestAuthorTransaction(t *testing.T) {
	ctx := context.Background()
	tx, err := testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txdb := txAdapter{tx}

	temp := &Author{FirstName: "Temp", LastName: "User", CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}}
	if err := temp.Insert(ctx, txdb); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert in tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := AuthorByAuthorID(ctx, connAdapter{testDB}, temp.AuthorID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected rollback to remove temp author, got %v", err)
	}

	tx, err = testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	txdb = txAdapter{tx}
	final := &Author{FirstName: "Commit", LastName: "User", CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}}
	if err := final.Insert(ctx, txdb); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert final: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.authors WHERE author_id=$1", final.AuthorID)
	if _, err := AuthorByAuthorID(ctx, connAdapter{testDB}, final.AuthorID); err != nil {
		t.Fatalf("committed author missing: %v", err)
	}
}

func TestBookCRUD(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	author := &Author{FirstName: "Book", LastName: "Author", CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}}
	if err := author.Insert(ctx, adapter); err != nil {
		t.Fatalf("create author: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.authors WHERE author_id=$1", author.AuthorID)

	price := pgtype.Numeric{Int: big.NewInt(1999), Exp: -2, Valid: true}
	book := &Book{
		AuthorID:      author.AuthorID,
		Title:         "Test Driven Go",
		PublishedYear: pgtype.Int2{Int16: 2023, Valid: true},
		Isbn:          "ISBN-TEST-1",
		Price:         price,
	}
	if err := book.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert book: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.books WHERE book_id=$1", book.BookID)

	fetched, err := BookByBookID(ctx, adapter, book.BookID)
	if err != nil {
		t.Fatalf("fetch book: %v", err)
	}
	if fetched.Title != book.Title || fetched.Isbn != book.Isbn {
		t.Fatalf("book mismatch: %#v", fetched)
	}

	fetched.Title = "Refined Go"
	if err := fetched.Update(ctx, adapter); err != nil {
		t.Fatalf("update book: %v", err)
	}
	updated, err := BookByBookID(ctx, adapter, book.BookID)
	if err != nil {
		t.Fatalf("refetch book: %v", err)
	}
	if updated.Title != "Refined Go" {
		t.Fatalf("title not updated: %#v", updated)
	}

	if _, err := BookByBookID(ctx, adapter, -1); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for missing book: %v", err)
	}

	if err := fetched.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete book: %v", err)
	}
	if _, err := BookByBookID(ctx, adapter, book.BookID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected missing after delete: %v", err)
	}
}

func TestBookTransaction(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	author := &Author{FirstName: "Txn", LastName: "Author", CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}}
	if err := author.Insert(ctx, adapter); err != nil {
		t.Fatalf("create author: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.authors WHERE author_id=$1", author.AuthorID)

	tx, err := testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txdb := txAdapter{tx}
	book := &Book{AuthorID: author.AuthorID, Title: "Rollback Book", PublishedYear: pgtype.Int2{Int16: 2020, Valid: true}, Isbn: "TXN-ROLL", Price: pgtype.Numeric{Int: big.NewInt(1000), Exp: -2, Valid: true}}
	if err := book.Insert(ctx, txdb); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert rollback book: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := BookByBookID(ctx, adapter, book.BookID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected rollback to remove book: %v", err)
	}

	tx, err = testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	txdb = txAdapter{tx}
	commitBook := &Book{AuthorID: author.AuthorID, Title: "Commit Book", PublishedYear: pgtype.Int2{Int16: 2021, Valid: true}, Isbn: "TXN-COMMIT", Price: pgtype.Numeric{Int: big.NewInt(2500), Exp: -2, Valid: true}}
	if err := commitBook.Insert(ctx, txdb); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert commit book: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.books WHERE book_id=$1", commitBook.BookID)
	if _, err := BookByBookID(ctx, adapter, commitBook.BookID); err != nil {
		t.Fatalf("committed book missing: %v", err)
	}
}

func TestTagCRUD(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	tag := &Tag{TagName: "go"}
	if err := tag.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert tag: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.tags WHERE tag_id=$1", tag.TagID)

	fetched, err := TagByTagID(ctx, adapter, tag.TagID)
	if err != nil {
		t.Fatalf("fetch tag: %v", err)
	}
	if fetched.TagName != tag.TagName {
		t.Fatalf("tag mismatch: %#v", fetched)
	}

	fetched.TagName = "go-updated"
	if err := fetched.Update(ctx, adapter); err != nil {
		t.Fatalf("update tag: %v", err)
	}
	updated, _ := TagByTagID(ctx, adapter, tag.TagID)
	if updated.TagName != "go-updated" {
		t.Fatalf("tag not updated: %#v", updated)
	}

	if _, err := TagByTagID(ctx, adapter, -1); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for missing tag: %v", err)
	}

	if err := fetched.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete tag: %v", err)
	}
	if _, err := TagByTagID(ctx, adapter, tag.TagID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected missing after delete: %v", err)
	}
}

func TestBookTagCRUD(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	author := &Author{FirstName: "Tag", LastName: "Author", CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}}
	if err := author.Insert(ctx, adapter); err != nil {
		t.Fatalf("create author: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.authors WHERE author_id=$1", author.AuthorID)

	book := &Book{AuthorID: author.AuthorID, Title: "Tagged", PublishedYear: pgtype.Int2{Int16: 2022, Valid: true}, Isbn: "TAGGED-1", Price: pgtype.Numeric{Int: big.NewInt(3000), Exp: -2, Valid: true}}
	if err := book.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert book: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.books WHERE book_id=$1", book.BookID)

	tag := &Tag{TagName: "fiction"}
	if err := tag.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert tag: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.tags WHERE tag_id=$1", tag.TagID)

	bt := &BookTag{BookID: book.BookID, TagID: tag.TagID}
	if err := bt.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert booktag: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.book_tags WHERE book_id=$1 AND tag_id=$2", bt.BookID, bt.TagID)

	fetched, err := BookTagByBookIDTagID(ctx, adapter, bt.BookID, bt.TagID)
	if err != nil {
		t.Fatalf("fetch booktag: %v", err)
	}
	if fetched.BookID != bt.BookID || fetched.TagID != bt.TagID {
		t.Fatalf("booktag mismatch: %#v", fetched)
	}

	if err := bt.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete booktag: %v", err)
	}
	if _, err := BookTagByBookIDTagID(ctx, adapter, bt.BookID, bt.TagID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected missing after delete: %v", err)
	}
}

func TestAdvancedFeatureCRUD(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	feature := &AdvancedFeature{
		IntArray:      []int{1, 2, 3},
		TextArray:     []string{"a", "b"},
		ProcessStatus: "PENDING",
		PointLocation: pgtype.Point{P: pgtype.Vec2{X: 1.1, Y: 2.2}, Valid: true},
		IntRange: pgtype.Range[pgtype.Int4]{
			Lower:     pgtype.Int4{Int32: 1, Valid: true},
			Upper:     pgtype.Int4{Int32: 10, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		FileData:     []byte("data"),
		EmailAddress: pgtype.Text{String: "test@example.com", Valid: true},
	}
	if err := feature.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert feature: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.advanced_features WHERE feature_id=$1", feature.FeatureID)

	fetched, err := AdvancedFeatureByFeatureID(ctx, adapter, feature.FeatureID)
	if err != nil {
		t.Fatalf("fetch feature: %v", err)
	}
	if fetched.ProcessStatus != feature.ProcessStatus || !bytes.Equal(fetched.FileData, feature.FileData) {
		t.Fatalf("feature mismatch: %#v", fetched)
	}

	fetched.ProcessStatus = "COMPLETED"
	fetched.EmailAddress = pgtype.Text{Valid: false}
	if err := fetched.Update(ctx, adapter); err != nil {
		t.Fatalf("update feature: %v", err)
	}
	updated, _ := AdvancedFeatureByFeatureID(ctx, adapter, feature.FeatureID)
	if updated.ProcessStatus != "COMPLETED" || updated.EmailAddress.Valid {
		t.Fatalf("update not persisted: %#v", updated)
	}

	if _, err := AdvancedFeatureByFeatureID(ctx, adapter, -1); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for missing feature: %v", err)
	}

	if err := fetched.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete feature: %v", err)
	}
	if _, err := AdvancedFeatureByFeatureID(ctx, adapter, feature.FeatureID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected missing after delete: %v", err)
	}
}

func TestCoreDataTypeCRUD(t *testing.T) {
	ctx := context.Background()
	adapter := connAdapter{testDB}
	jsonData := json.RawMessage(`{"key":"value"}`)
	nullableJSON := json.RawMessage(`{"nullable":true}`)
	record := &CoreDataType{
		SmallIntVal:        1,
		IntegerVal:         2,
		BigIntVal:          3,
		NumericVal:         pgtype.Numeric{Int: big.NewInt(12345), Exp: -2, Valid: true},
		RealVal:            pgtype.Float4{Float32: 1.5, Valid: true},
		DoublePrecisionVal: pgtype.Float8{Float64: 2.5, Valid: true},
		CharVal:            "c",
		VarcharVal:         "varchar",
		TextVal:            pgtype.Text{String: "text", Valid: true},
		BooleanVal:         true,
		DateVal:            time.Now().UTC(),
		TimeVal: func() pgtype.Time {
			now := time.Now().UTC()
			midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			return pgtype.Time{Microseconds: now.Sub(midnight).Microseconds(), Valid: true}
		}(),
		TimestampVal:      time.Now().UTC(),
		TimestamptzVal:    time.Now().UTC(),
		IntervalVal:       pgtype.Interval{Microseconds: 1000, Valid: true},
		UUIDVal:           pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true},
		JsonbDataNullable: &nullableJSON,
		JsonbData:         jsonData,
		NullableInt:       pgtype.Int4{Int32: 9, Valid: true},
		NullableText:      pgtype.Text{String: "maybe", Valid: true},
                NullableTime:      pgtype.Time{Valid: false},
	}
	if err := record.Insert(ctx, adapter); err != nil {
		t.Fatalf("insert coredata: %v", err)
	}
	defer testDB.Exec(ctx, "DELETE FROM public.core_data_types WHERE id=$1", record.ID)

	fetched, err := CoreDataTypeByID(ctx, adapter, record.ID)
	if err != nil {
		t.Fatalf("fetch coredata: %v", err)
	}
	if fetched.NumericVal.Int.Cmp(record.NumericVal.Int) != 0 || fetched.TextVal.String != record.TextVal.String {
		t.Fatalf("core data mismatch: %#v", fetched)
	}

	fetched.NullableInt = pgtype.Int4{Valid: false}
	fetched.VarcharVal = ""
	if err := fetched.Update(ctx, adapter); err != nil {
		t.Fatalf("update coredata: %v", err)
	}
	updated, _ := CoreDataTypeByID(ctx, adapter, record.ID)
	if updated.NullableInt.Valid || updated.VarcharVal != "" {
		t.Fatalf("update not persisted: %#v", updated)
	}

	if _, err := CoreDataTypeByID(ctx, adapter, -1); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for missing coredata: %v", err)
	}

	if err := fetched.Delete(ctx, adapter); err != nil {
		t.Fatalf("delete coredata: %v", err)
	}
	if _, err := CoreDataTypeByID(ctx, adapter, record.ID); err == nil || !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected missing after delete: %v", err)
	}
}
