//go:build dbtpl

// Package pgx contains the pgx template for dbtpl.
package pgx

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"

	"github.com/kenshaw/inflector"
	"github.com/kenshaw/snaker"
	"github.com/xo/dbtpl/loader"
	xo "github.com/xo/dbtpl/types"
	"golang.org/x/tools/imports"
	"mvdan.cc/gofumpt/format"
)

// Init registers the pgx template.
func Init(ctx context.Context, f func(xo.TemplateType)) error {
	knownTypes := map[string]bool{
		"bool":    true,
		"string":  true,
		"byte":    true,
		"rune":    true,
		"int":     true,
		"int16":   true,
		"int32":   true,
		"int64":   true,
		"uint":    true,
		"uint8":   true,
		"uint16":  true,
		"uint32":  true,
		"uint64":  true,
		"float32": true,
		"float64": true,
		"[]byte":  true,
		// pgtype types
		"pgtype.Bool":        true,
		"pgtype.Int2":        true,
		"pgtype.Int4":        true,
		"pgtype.Int8":        true,
		"pgtype.Float4":      true,
		"pgtype.Float8":      true,
		"pgtype.Numeric":     true,
		"pgtype.Text":        true,
		"pgtype.Timestamp":   true,
		"pgtype.Timestamptz": true,
		"pgtype.Date":        true,
		"pgtype.Time":        true,
		"pgtype.Interval":    true,
		"pgtype.UUID":        true,
		"time.Time":          true,
		// array types
		"[]bool":    true,
		"[]int16":   true,
		"[]int32":   true,
		"[]int64":   true,
		"[]float32": true,
		"[]float64": true,
		"[]string":  true,
	}
	shorts := map[string]string{
		"bool":               "b",
		"string":             "s",
		"byte":               "b",
		"rune":               "r",
		"int":                "i",
		"int16":              "i",
		"int32":              "i",
		"int64":              "i",
		"uint":               "u",
		"uint8":              "u",
		"uint16":             "u",
		"uint32":             "u",
		"uint64":             "u",
		"float32":            "f",
		"float64":            "f",
		"[]byte":             "b",
		"pgtype.Bool":        "b",
		"pgtype.Int2":        "i",
		"pgtype.Int4":        "i",
		"pgtype.Int8":        "i",
		"pgtype.Float4":      "f",
		"pgtype.Float8":      "f",
		"pgtype.Numeric":     "n",
		"pgtype.Text":        "t",
		"pgtype.Timestamp":   "t",
		"pgtype.Timestamptz": "t",
		"pgtype.Date":        "d",
		"pgtype.Time":        "t",
		"pgtype.Interval":    "i",
		"pgtype.UUID":        "u",
		"time.Time":          "t",
		"[]bool":             "a",
		"[]int16":            "a",
		"[]int32":            "a",
		"[]int64":            "a",
		"[]float32":          "a",
		"[]float64":          "a",
		"[]string":           "a",
	}
	f(xo.TemplateType{
		Modes: []string{"query", "schema"},
		Flags: []xo.Flag{
			{
				ContextKey: NotFirstKey,
				Type:       "bool",
				Desc:       "disable package file (i.e. not first generated file)",
				Short:      "2",
			},
			{
				ContextKey: Int32Key,
				Type:       "string",
				Desc:       "int32 type",
				Default:    "int",
			},
			{
				ContextKey: Uint32Key,
				Type:       "string",
				Desc:       "uint32 type",
				Default:    "uint",
			},
			{
				ContextKey: PkgKey,
				Type:       "string",
				Desc:       "package name",
			},
			{
				ContextKey: TagKey,
				Type:       "[]string",
				Desc:       "build tags",
			},
			{
				ContextKey: ImportKey,
				Type:       "[]string",
				Desc:       "package imports",
			},
			{
				ContextKey: UUIDKey,
				Type:       "string",
				Desc:       "uuid type package",
				Default:    "github.com/google/uuid",
			},
			{
				ContextKey: CustomKey,
				Type:       "string",
				Desc:       "package name for custom types",
			},
			{
				ContextKey: ConflictKey,
				Type:       "string",
				Desc:       "name conflict suffix",
				Default:    "Val",
			},
			{
				ContextKey: InitialismKey,
				Type:       "[]string",
				Desc:       "add initialism (e.g. ID, API, URI, ...)",
			},
			{
				ContextKey: EscKey,
				Type:       "[]string",
				Desc:       "escape fields",
				Default:    "none",
				Enums:      []string{"none", "schema", "table", "column", "all"},
			},
			{
				ContextKey: FieldTagKey,
				Type:       "string",
				Desc:       "field tag",
				Short:      "g",
				Default:    `json:"{{ .SQLName }}" db:"{{ .SQLName }}"`,
			},
			{
				ContextKey: ContextKey,
				Type:       "string",
				Desc:       "context mode",
				Enums:      []string{"only", "disable", "both"},
			},
			{
				ContextKey: InjectKey,
				Type:       "string",
				Desc:       "insert code into generated file headers",
				Default:    "",
			},
			{
				ContextKey: InjectFileKey,
				Type:       "string",
				Desc:       "insert code into generated file headers from a file",
				Default:    "",
			},
		},
		Funcs: func(ctx context.Context, _ string) (template.FuncMap, error) {
			return NewFuncs(ctx)
		},
		NewContext: func(ctx context.Context, _ string) context.Context {
			ctx = context.WithValue(ctx, KnownTypesKey, knownTypes)
			ctx = context.WithValue(ctx, ShortsKey, shorts)
			return ctx
		},
		Order: func(ctx context.Context, mode string) []string {
			base := []string{"header", "db"}
			switch mode {
			case "query":
				return append(base, "typedef", "query")
			case "schema":
				return append(base, "enum", "proc", "typedef", "query", "index", "foreignkey")
			}
			return nil
		},
		Pre: func(ctx context.Context, mode string, set *xo.Set, out fs.FS, emit func(xo.Template)) error {
			if err := addInitialisms(ctx); err != nil {
				return err
			}
			// Check driver is postgres
			driver, _, _ := xo.DriverDbSchema(ctx)
			if driver != "postgres" {
				return fmt.Errorf("pgx template only supports postgres driver, got %q", driver)
			}
			files, err := fileNames(ctx, mode, set)
			if err != nil {
				return err
			}
			// If -2 is provided, skip package template outputs as requested.
			// If -a is provided, skip to avoid duplicating the template.
			if !NotFirst(ctx) && !Append(ctx) {
				emit(xo.Template{
					Partial: "db",
					Dest:    "dbtpl.dbtpl.go",
				})
				// If --single is provided, don't generate header for db.dbtpl.go.
				if xo.Single(ctx) == "" {
					emit(xo.Template{
						Partial: "header",
						Dest:    "dbtpl.dbtpl.go",
					})
				}
			}
			// Add headers to all files.
			for _, file := range files {
				emit(xo.Template{
					Partial: "header",
					Dest:    file,
				})
			}
			return nil
		},
		Process: func(ctx context.Context, mode string, set *xo.Set, emit func(xo.Template)) error {
			switch mode {
			case "schema":
				for _, schema := range set.Schemas {
					if err := emitSchema(ctx, schema, emit); err != nil {
						return err
					}
				}
			case "query":
				for _, query := range set.Queries {
					if err := emitQuery(ctx, query, emit); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Post: func(ctx context.Context, mode string, files map[string][]byte, emit func(string, []byte)) error {
			for file, content := range files {
				// Run goimports
				buf, err := imports.Process(file, content, nil)
				if err != nil {
					return fmt.Errorf("unable to goimports %s: %w", file, err)
				}
				// Run gofumpt
				buf, err = format.Source(buf, format.Options{
					ExtraRules: true,
				})
				if err != nil {
					return fmt.Errorf("unable to gofumpt %s: %w", file, err)
				}
				emit(file, buf)
			}
			return nil
		},
	})
	return nil
}

// Context keys.
var (
	NotFirstKey   xo.ContextKey = "not-first"
	Int32Key      xo.ContextKey = "int32"
	Uint32Key     xo.ContextKey = "uint32"
	PkgKey        xo.ContextKey = "pkg"
	TagKey        xo.ContextKey = "tag"
	ImportKey     xo.ContextKey = "import"
	UUIDKey       xo.ContextKey = "uuid"
	CustomKey     xo.ContextKey = "custom"
	ConflictKey   xo.ContextKey = "conflict"
	InitialismKey xo.ContextKey = "initialism"
	EscKey        xo.ContextKey = "esc"
	FieldTagKey   xo.ContextKey = "field-tag"
	ContextKey    xo.ContextKey = "context"
	InjectKey     xo.ContextKey = "inject"
	InjectFileKey xo.ContextKey = "inject-file"
	KnownTypesKey xo.ContextKey = "known-types"
	ShortsKey     xo.ContextKey = "shorts"
)

// NotFirst returns not-first from the context.
func NotFirst(ctx context.Context) bool {
	b, _ := ctx.Value(NotFirstKey).(bool)
	return b
}

// Int32 returns int32 from the context.
func Int32(ctx context.Context) string {
	s, _ := ctx.Value(Int32Key).(string)
	return s
}

// Uint32 returns uint32 from the context.
func Uint32(ctx context.Context) string {
	s, _ := ctx.Value(Uint32Key).(string)
	return s
}

// Append returns append from the context.
func Append(ctx context.Context) bool {
	return xo.Append(ctx)
}

// Pkg returns pkg from the context.
func Pkg(ctx context.Context) string {
	s, _ := ctx.Value(PkgKey).(string)
	if s == "" {
		s = filepath.Base(xo.Out(ctx))
	}
	return s
}

// Tags returns tags from the context.
func Tags(ctx context.Context) []string {
	v, _ := ctx.Value(TagKey).([]string)
	return v
}

// Imports returns imports from the context.
func Imports(ctx context.Context) []string {
	v, _ := ctx.Value(ImportKey).([]string)
	return v
}

// UUID returns uuid from the context.
func UUID(ctx context.Context) string {
	s, _ := ctx.Value(UUIDKey).(string)
	return s
}

// Custom returns custom from the context.
func Custom(ctx context.Context) string {
	s, _ := ctx.Value(CustomKey).(string)
	return s
}

// Conflict returns conflict from the context.
func Conflict(ctx context.Context) string {
	s, _ := ctx.Value(ConflictKey).(string)
	return s
}

// Esc checks if esc contains any of the modes.
func Esc(ctx context.Context, modes ...string) bool {
	v, _ := ctx.Value(EscKey).([]string)
	for _, mode := range modes {
		if slices.Contains(v, mode) || slices.Contains(v, "all") {
			return true
		}
	}
	return false
}

// FieldTag returns field-tag from the context.
func FieldTag(ctx context.Context) string {
	s, _ := ctx.Value(FieldTagKey).(string)
	return s
}

// Context returns context from the context.
func Context(ctx context.Context) string {
	s, _ := ctx.Value(ContextKey).(string)
	return s
}

// Inject returns inject from the context.
func Inject(ctx context.Context) string {
	s, _ := ctx.Value(InjectKey).(string)
	return s
}

// InjectFile returns inject-file from the context.
func InjectFile(ctx context.Context) string {
	s, _ := ctx.Value(InjectFileKey).(string)
	return s
}

// KnownTypes returns known-types from the context.
func KnownTypes(ctx context.Context) map[string]bool {
	m, _ := ctx.Value(KnownTypesKey).(map[string]bool)
	return m
}

// Shorts returns shorts from the context.
func Shorts(ctx context.Context) map[string]string {
	m, _ := ctx.Value(ShortsKey).(map[string]string)
	return m
}

// addInitialisms adds snaker initialisms from the context.
func addInitialisms(ctx context.Context) error {
	var v []string
	for _, s := range ctx.Value(InitialismKey).([]string) {
		if s != "" {
			v = append(v, s)
		}
	}
	return snaker.DefaultInitialisms.Add(v...)
}

// singularize singularizes s.
func singularize(s string) string {
	if i := strings.LastIndex(s, "_"); i != -1 {
		return s[:i+1] + inflector.Singularize(s[i+1:])
	}
	return inflector.Singularize(s)
}

// Template types

// EnumValue is a enum value template.
type EnumValue struct {
	GoName     string
	SQLName    string
	ConstValue int
}

// Enum is a enum type template.
type Enum struct {
	GoName  string
	SQLName string
	Values  []EnumValue
	Comment string
}

// Proc is a stored procedure template.
type Proc struct {
	Type           string
	GoName         string
	OverloadedName string
	SQLName        string
	Signature      string
	Params         []Field
	Returns        []Field
	Void           bool
	Overloaded     bool
	Comment        string
}

// Table is a type (i.e., table/view/custom query) template.
type Table struct {
	Type        string
	GoName      string
	SQLName     string
	PrimaryKeys []Field
	Fields      []Field
	Manual      bool
	Comment     string
}

// ForeignKey is a foreign key template.
type ForeignKey struct {
	GoName    string
	SQLName   string
	Table     Table
	Fields    []Field
	RefTable  string
	RefFields []Field
	RefFunc   string
	Comment   string
}

// Index is an index template.
type Index struct {
	SQLName   string
	Func      string
	Table     Table
	Fields    []Field
	IsUnique  bool
	IsPrimary bool
	Comment   string
}

// Field is a field template.
type Field struct {
	GoName     string
	SQLName    string
	Type       string
	Zero       string
	IsPrimary  bool
	IsSequence bool
	Comment    string
}

// QueryParam is a custom query parameter template.
type QueryParam struct {
	Name        string
	Type        string
	Interpolate bool
	Join        bool
}

// Query is a custom query template.
type Query struct {
	Name        string
	Query       []string
	Comments    []string
	Params      []QueryParam
	One         bool
	Flat        bool
	Exec        bool
	Interpolate bool
	Type        Table
	Comment     string
}

const ext = ".dbtpl.go"

// fileNames returns the file names for the set.
func fileNames(ctx context.Context, mode string, set *xo.Set) ([]string, error) {
	var files []string
	switch mode {
	case "schema":
		for _, schema := range set.Schemas {
			// enums
			for _, e := range schema.Enums {
				files = append(files, strings.ToLower(camelExport(e.Name))+ext)
			}
			// procs
			procs := make(map[string]bool)
			for _, p := range schema.Procs {
				name := camelExport(p.Name)
				if !procs[name] {
					procs[name] = true
					prefix := "sp_"
					if p.Type == "function" {
						prefix = "sf_"
					}
					files = append(files, prefix+strings.ToLower(name)+ext)
				}
			}
			// tables - use singularize to match emitSchema
			for _, t := range append(schema.Tables, schema.Views...) {
				files = append(files, strings.ToLower(camelExport(singularize(t.Name)))+ext)
			}
		}
	case "query":
		for _, q := range set.Queries {
			files = append(files, strings.ToLower(camelExport(q.Type))+ext)
		}
	}
	return files, nil
}

// buildQueryType builds a Table for a query type (without singularization).
func buildQueryType(ctx context.Context, query xo.Query) (Table, error) {
	tf := camelExport
	if query.Flat {
		tf = camel
	}
	var fields []Field
	for _, z := range query.Fields {
		f, err := convertField(ctx, tf, z)
		if err != nil {
			return Table{}, err
		}
		// don't use convertField types if ManualFields; types are provided by user
		if query.ManualFields {
			f = Field{
				GoName:  z.Name,
				SQLName: snaker.CamelToSnake(z.Name),
				Type:    z.Type.Type,
			}
		}
		fields = append(fields, f)
	}
	sqlName := snaker.CamelToSnake(query.Type)
	return Table{
		GoName:  query.Type, // NOT singularized for query types
		SQLName: sqlName,
		Fields:  fields,
	}, nil
}

// emitQuery emits a query.
func emitQuery(ctx context.Context, query xo.Query, emit func(xo.Template)) error {
	var typ Table
	// build type if needed
	if !query.Exec {
		var err error
		if typ, err = buildQueryType(ctx, query); err != nil {
			return err
		}
	}
	// build params
	var params []QueryParam
	for _, p := range query.Params {
		params = append(params, QueryParam{
			Name:        p.Name,
			Type:        p.Type.Type,
			Interpolate: p.Interpolate,
			Join:        p.Join,
		})
	}
	// emit typedef if not flat and not exec
	if !query.Flat && !query.Exec {
		emit(xo.Template{
			Partial:  "typedef",
			Dest:     strings.ToLower(typ.GoName) + ext,
			SortType: query.Type,
			SortName: query.Name,
			Data:     typ,
		})
	}
	name := buildQueryName(query)
	emit(xo.Template{
		Partial:  "query",
		Dest:     strings.ToLower(typ.GoName) + ext,
		SortType: query.Type,
		SortName: query.Name,
		Data: Query{
			Name:        name,
			Query:       query.Query,
			Comments:    query.Comments,
			Params:      params,
			One:         query.Exec || query.Flat || query.One,
			Flat:        query.Flat,
			Exec:        query.Exec,
			Interpolate: query.Interpolate,
			Type:        typ,
			Comment:     query.Comment,
		},
	})
	return nil
}

// buildQueryName builds a name for a query.
func buildQueryName(query xo.Query) string {
	if query.Name != "" {
		return query.Name
	}
	// generate name if not specified
	name := query.Type
	if !query.One {
		name = inflector.Pluralize(name)
	}
	// add params
	if len(query.Params) == 0 {
		name = "Get" + name
	} else {
		name += "By"
		for _, p := range query.Params {
			name += camelExport(p.Name)
		}
	}
	return name
}

// emitSchema emits the schema for the template set.
func emitSchema(ctx context.Context, schema xo.Schema, emit func(xo.Template)) error {
	// emit enums
	for _, e := range schema.Enums {
		enum := convertEnum(e)
		emit(xo.Template{
			Partial:  "enum",
			Dest:     strings.ToLower(enum.GoName) + ext,
			SortName: enum.GoName,
			Data:     enum,
		})
	}
	// build procs
	overloadMap := make(map[string][]Proc)
	var procOrder []string
	for _, p := range schema.Procs {
		var err error
		if procOrder, err = convertProc(ctx, overloadMap, procOrder, p); err != nil {
			return err
		}
	}
	// emit procs
	for _, name := range procOrder {
		procs := overloadMap[name]
		prefix := "sp_"
		if procs[0].Type == "function" {
			prefix = "sf_"
		}
		// Set flag to change name to their overloaded versions if needed.
		for i := range procs {
			procs[i].Overloaded = len(procs) > 1
		}
		emit(xo.Template{
			Dest:     prefix + strings.ToLower(name) + ext,
			Partial:  "procs",
			SortName: prefix + name,
			Data:     procs,
		})
	}
	// emit tables
	for _, t := range append(schema.Tables, schema.Views...) {
		table, err := convertTable(ctx, t)
		if err != nil {
			return err
		}
		emit(xo.Template{
			Dest:     strings.ToLower(table.GoName) + ext,
			Partial:  "typedef",
			SortType: table.Type,
			SortName: table.GoName,
			Data:     table,
		})
		// emit indexes
		for _, i := range t.Indexes {
			index, err := convertIndex(ctx, table, i)
			if err != nil {
				return err
			}
			emit(xo.Template{
				Dest:     strings.ToLower(table.GoName) + ext,
				Partial:  "index",
				SortType: table.Type,
				SortName: index.SQLName,
				Data:     index,
			})
		}
		// emit fkeys
		for _, fk := range t.ForeignKeys {
			fkey, err := convertFKey(ctx, table, fk)
			if err != nil {
				return err
			}
			emit(xo.Template{
				Dest:     strings.ToLower(table.GoName) + ext,
				Partial:  "foreignkey",
				SortType: table.Type,
				SortName: fkey.SQLName,
				Data:     fkey,
			})
		}
	}
	return nil
}

// convertEnum converts a xo.Enum.
func convertEnum(e xo.Enum) Enum {
	var vals []EnumValue
	goName := camelExport(e.Name)
	for _, v := range e.Values {
		constVal := 0
		if v.ConstValue != nil {
			constVal = *v.ConstValue
		}
		// Handle enum value name - strip the enum type name suffix if present
		name := camelExport(strings.ToLower(v.Name))
		if strings.HasSuffix(name, goName) && goName != name {
			name = strings.TrimSuffix(name, goName)
		}
		vals = append(vals, EnumValue{
			GoName:     name,
			SQLName:    v.Name,
			ConstValue: constVal,
		})
	}
	return Enum{
		GoName:  goName,
		SQLName: e.Name,
		Values:  vals,
	}
}

// convertProc converts a xo.Proc.
func convertProc(ctx context.Context, overloadMap map[string][]Proc, procOrder []string, proc xo.Proc) ([]string, error) {
	// convert params
	params, err := convertFields(ctx, camel, proc.Params)
	if err != nil {
		return nil, err
	}
	// convert returns
	returns, err := convertFields(ctx, camel, proc.Returns)
	if err != nil {
		return nil, err
	}
	// build overloaded name
	oname := buildOverloadedName(proc)
	// add to map
	name := camelExport(proc.Name)
	if _, ok := overloadMap[name]; !ok {
		procOrder = append(procOrder, name)
	}
	overloadMap[name] = append(overloadMap[name], Proc{
		Type:           proc.Type,
		GoName:         name,
		OverloadedName: oname,
		SQLName:        proc.Name,
		Signature:      proc.Definition,
		Params:         params,
		Returns:        returns,
		Void:           proc.Void,
	})
	return procOrder, nil
}

// buildOverloadedName builds an overloaded name for a proc.
func buildOverloadedName(proc xo.Proc) string {
	var sqlTypes []string
	for _, p := range proc.Params {
		sqlTypes = append(sqlTypes, p.Type.Type)
	}
	var names []string
	for i, f := range proc.Params {
		if f.Name == fmt.Sprintf("p%d", i) {
			names = append(names, camelExport(strings.Split(sqlTypes[i], " ")...))
			continue
		}
		names = append(names, camelExport(f.Name))
	}
	if len(names) == 1 {
		return fmt.Sprintf("%sBy%s", proc.Name, names[0])
	}
	front, last := strings.Join(names[:len(names)-1], ""), names[len(names)-1]
	return fmt.Sprintf("%sBy%sAnd%s", proc.Name, front, last)
}

// convertTable converts a xo.Table.
func convertTable(ctx context.Context, t xo.Table) (Table, error) {
	// convert fields
	fields, err := convertFields(ctx, camelExport, t.Columns)
	if err != nil {
		return Table{}, err
	}
	// determine primary keys
	var primaryKeys []Field
	for _, f := range fields {
		if f.IsPrimary {
			primaryKeys = append(primaryKeys, f)
		}
	}
	typ := t.Type
	if typ == "" {
		typ = "table"
	}
	return Table{
		Type:        typ,
		GoName:      camelExport(singularize(t.Name)),
		SQLName:     t.Name,
		PrimaryKeys: primaryKeys,
		Fields:      fields,
		Manual:      t.Manual,
	}, nil
}

// convertFields converts a slice of xo.Field.
func convertFields(ctx context.Context, tf transformFunc, fields []xo.Field) ([]Field, error) {
	var res []Field
	for _, f := range fields {
		field, err := convertField(ctx, tf, f)
		if err != nil {
			return nil, err
		}
		res = append(res, field)
	}
	return res, nil
}

// convertField converts a xo.Field.
func convertField(ctx context.Context, tf transformFunc, f xo.Field) (Field, error) {
	typ, zero, err := pgxGoType(ctx, f.Type)
	if err != nil {
		return Field{}, err
	}
	return Field{
		Type:       typ,
		GoName:     tf(f.Name),
		SQLName:    f.Name,
		Zero:       zero,
		IsPrimary:  f.IsPrimary,
		IsSequence: f.IsSequence,
		Comment:    f.Comment,
	}, nil
}

// convertIndex converts a xo.Index.
func convertIndex(ctx context.Context, t Table, i xo.Index) (Index, error) {
	fields, err := convertFields(ctx, camelExport, i.Fields)
	if err != nil {
		return Index{}, err
	}
	return Index{
		SQLName:   i.Name,
		Func:      buildIndexFunc(t, i),
		Table:     t,
		Fields:    fields,
		IsUnique:  i.IsUnique,
		IsPrimary: i.IsPrimary,
	}, nil
}

// buildIndexFunc builds the index func name.
func buildIndexFunc(t Table, i xo.Index) string {
	name := t.GoName + "By"
	for _, f := range i.Fields {
		name += camelExport(f.Name)
	}
	return name
}

// convertFKey converts a xo.ForeignKey.
func convertFKey(ctx context.Context, t Table, fk xo.ForeignKey) (ForeignKey, error) {
	fields, err := convertFields(ctx, camelExport, fk.Fields)
	if err != nil {
		return ForeignKey{}, err
	}
	refFields, err := convertFields(ctx, camelExport, fk.RefFields)
	if err != nil {
		return ForeignKey{}, err
	}
	return ForeignKey{
		GoName:    buildFKeyFunc(t, fk),
		SQLName:   fk.Name,
		Table:     t,
		Fields:    fields,
		RefTable:  camelExport(singularize(fk.RefTable)),
		RefFields: refFields,
		RefFunc:   camelExport(singularize(fk.RefTable)) + "By" + buildRefFieldNames(fk.RefFields),
	}, nil
}

// buildFKeyFunc builds the foreign key func name.
func buildFKeyFunc(t Table, fk xo.ForeignKey) string {
	return camelExport(singularize(fk.RefTable)) + "By" + buildFieldNames(fk.Fields)
}

// buildFieldNames builds a string of field names.
func buildFieldNames(fields []xo.Field) string {
	var names []string
	for _, f := range fields {
		names = append(names, camelExport(f.Name))
	}
	return strings.Join(names, "")
}

// buildRefFieldNames builds a string of ref field names.
func buildRefFieldNames(fields []xo.Field) string {
	var names []string
	for _, f := range fields {
		names = append(names, camelExport(f.Name))
	}
	return strings.Join(names, "")
}

type transformFunc func(...string) string

func snake(names ...string) string {
	return snaker.CamelToSnake(strings.Join(names, "_"))
}

func camel(names ...string) string {
	return snaker.ForceLowerCamelIdentifier(strings.Join(names, "_"))
}

func camelExport(names ...string) string {
	return snaker.ForceCamelIdentifier(strings.Join(names, "_"))
}

// pgxGoType converts a database type to a pgx-compatible Go type.
func pgxGoType(ctx context.Context, typ xo.Type) (string, string, error) {
	_, _, schema := xo.DriverDbSchema(ctx)
	itype := Int32(ctx)
	// SETOF -> []T
	if strings.HasPrefix(typ.Type, "SETOF ") {
		typ.Type = typ.Type[len("SETOF "):]
		goType, _, err := pgxGoType(ctx, typ)
		if err != nil {
			return "", "", err
		}
		return "[]" + goType, "nil", nil
	}
	// If it's an array, the underlying type shouldn't also be set as an array
	typNullable := typ.Nullable && !typ.IsArray
	// special type handling
	dbType := typ.Type
	switch {
	case dbType == `"char"`:
		dbType = "char"
	case strings.HasPrefix(dbType, "information_schema."):
		switch strings.TrimPrefix(dbType, "information_schema.") {
		case "cardinal_number":
			dbType = "integer"
		case "character_data", "sql_identifier", "yes_or_no":
			dbType = "character varying"
		case "time_stamp":
			dbType = "timestamp with time zone"
		}
	}
	var goType, zero string
	switch dbType {
	case "boolean":
		goType, zero = "bool", "false"
		if typNullable {
			goType, zero = "pgtype.Bool", "pgtype.Bool{}"
		}
	case "bpchar", "character varying", "character", "inet", "money", "text", "name":
		goType, zero = "string", `""`
		if typNullable {
			goType, zero = "pgtype.Text", "pgtype.Text{}"
		}
	case "smallint":
		goType, zero = "int16", "0"
		if typNullable {
			goType, zero = "pgtype.Int2", "pgtype.Int2{}"
		}
	case "integer":
		goType, zero = itype, "0"
		if typNullable {
			goType, zero = "pgtype.Int4", "pgtype.Int4{}"
		}
	case "bigint":
		goType, zero = "int64", "0"
		if typNullable {
			goType, zero = "pgtype.Int8", "pgtype.Int8{}"
		}
	case "real":
		goType, zero = "float32", "0.0"
		if typNullable {
			goType, zero = "pgtype.Float4", "pgtype.Float4{}"
		}
	case "double precision", "numeric":
		goType, zero = "float64", "0.0"
		if typNullable {
			goType, zero = "pgtype.Float8", "pgtype.Float8{}"
		}
	case "date":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "pgtype.Date", "pgtype.Date{}"
		}
	case "timestamp without time zone":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "pgtype.Timestamp", "pgtype.Timestamp{}"
		}
	case "timestamp with time zone":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "pgtype.Timestamptz", "pgtype.Timestamptz{}"
		}
	case "time without time zone", "time with time zone":
		goType, zero = "time.Time", "time.Time{}"
		if typNullable {
			goType, zero = "pgtype.Time", "pgtype.Time{}"
		}
	case "bit":
		goType, zero = "uint8", "0"
		if typNullable {
			goType, zero = "*uint8", "nil"
		}
	case "any", "bit varying", "bytea", "interval", "json", "jsonb", "xml":
		goType, zero = "[]byte", "nil"
	case "hstore":
		goType, zero = "map[string]string", "nil"
	case "uuid":
		goType, zero = "pgtype.UUID", "pgtype.UUID{}"
	default:
		goType, zero = schemaType(typ.Type, typNullable, schema)
	}
	// handle array types
	if typ.IsArray {
		arrType, ok := pgxArrMapping[goType]
		if ok {
			goType, zero = arrType, "nil"
		} else {
			goType, zero = "[]byte", "nil"
		}
	}
	return goType, zero, nil
}

// pgxArrMapping is the mapping for pgx array types.
var pgxArrMapping = map[string]string{
	"bool":    "[]bool",
	"int16":   "[]int16",
	"int32":   "[]int32",
	"int":     "[]int",
	"int64":   "[]int64",
	"float32": "[]float32",
	"float64": "[]float64",
	"string":  "[]string",
}

// schemaType returns the Go type for a schema type.
func schemaType(typ string, nullable bool, schema string) (string, string) {
	goType := camelExport(typ)
	if nullable {
		goType = "Null" + goType
	}
	return goType, goType + "{}"
}

// Funcs is the set of template functions.
type Funcs struct {
	driver    string
	schema    string
	nth       func(int) string
	first     bool
	pkg       string
	tags      []string
	imports   []string
	conflict  string
	custom    string
	escSchema bool
	escTable  bool
	escColumn bool
	fieldtag  *template.Template
	context   string
	inject    string
	// knownTypes is the collection of known Go types.
	knownTypes map[string]bool
	// shorts is the collection of Go style short names for types.
	shorts map[string]string
}

// NewFuncs creates custom template funcs for the context.
func NewFuncs(ctx context.Context) (template.FuncMap, error) {
	first := !NotFirst(ctx)
	// parse field tag template
	fieldtag, err := template.New("fieldtag").Parse(FieldTag(ctx))
	if err != nil {
		return nil, err
	}
	// load inject
	inject := Inject(ctx)
	if s := InjectFile(ctx); s != "" {
		buf, err := os.ReadFile(s)
		if err != nil {
			return nil, fmt.Errorf("unable to read file: %v", err)
		}
		inject = string(buf)
	}
	driver, _, schema := xo.DriverDbSchema(ctx)
	nth, err := loader.NthParam(ctx)
	if err != nil {
		return nil, err
	}
	funcs := &Funcs{
		first:      first,
		driver:     driver,
		schema:     schema,
		nth:        nth,
		pkg:        Pkg(ctx),
		tags:       Tags(ctx),
		imports:    Imports(ctx),
		conflict:   Conflict(ctx),
		custom:     Custom(ctx),
		escSchema:  Esc(ctx, "schema"),
		escTable:   Esc(ctx, "table"),
		escColumn:  Esc(ctx, "column"),
		fieldtag:   fieldtag,
		context:    Context(ctx),
		inject:     inject,
		knownTypes: KnownTypes(ctx),
		shorts:     Shorts(ctx),
	}
	return funcs.FuncMap(), nil
}

// FuncMap returns the func map.
func (f *Funcs) FuncMap() template.FuncMap {
	return template.FuncMap{
		// general
		"first":   f.firstfn,
		"driver":  f.driverfn,
		"schema":  f.schemafn,
		"pkg":     f.pkgfn,
		"tags":    f.tagsfn,
		"imports": f.importsfn,
		"inject":  f.injectfn,
		// context
		"context":         f.contextfn,
		"context_both":    f.context_both,
		"context_disable": f.context_disable,
		// func and query
		"func_name_context":   f.func_name_context,
		"func_name":           f.func_name_none,
		"func_context":        f.func_context,
		"func":                f.func_none,
		"recv_context":        f.recv_context,
		"recv":                f.recv_none,
		"foreign_key_context": f.foreign_key_context,
		"foreign_key":         f.foreign_key_none,
		"db":                  f.db,
		"db_prefix":           f.db_prefix,
		"db_update":           f.db_update,
		"logf":                f.logf,
		"logf_pkeys":          f.logf_pkeys,
		"logf_update":         f.logf_update,
		// type
		"names":        f.names,
		"names_all":    f.names_all,
		"names_ignore": f.names_ignore,
		"params":       f.params,
		"zero":         f.zero,
		"type":         f.typefn,
		"field":        f.field,
		"short":        f.short,
		// sqlstr funcs
		"querystr": f.querystr,
		"sqlstr":   f.sqlstr,
		// helpers
		"check_name": checkName,
		"eval":       eval,
	}
}

func (f *Funcs) firstfn() bool {
	if f.first {
		f.first = false
		return true
	}
	return false
}

// driverfn returns true if the driver is any of the passed drivers.
func (f *Funcs) driverfn(drivers ...string) bool {
	for _, driver := range drivers {
		if f.driver == driver {
			return true
		}
	}
	return false
}

// schemafn takes a series of names and joins them with the schema name.
func (f *Funcs) schemafn(names ...string) string {
	s, n := f.schema, strings.Join(names, ".")
	switch {
	case s == "" && n == "":
		return ""
	case f.driver == "sqlite3":
		return n
	}
	if s != "" && n != "" && f.escSchema {
		return `"` + s + `"."` + n + `"`
	}
	if s != "" && n != "" {
		return s + "." + n
	}
	if f.escSchema {
		return `"` + n + `"`
	}
	return n
}

func (f *Funcs) pkgfn() string {
	return f.pkg
}

func (f *Funcs) tagsfn() []string {
	return f.tags
}

func (f *Funcs) importsfn() []Import {
	var imports []Import
	for _, s := range f.imports {
		alias, pkg := "", s
		if i := strings.Index(s, " "); i != -1 {
			alias, pkg = s[:i], s[i+1:]
		}
		imports = append(imports, Import{
			Alias: alias,
			Pkg:   pkg,
		})
	}
	return imports
}

// Import is an import.
type Import struct {
	Alias string
	Pkg   string
}

func (f *Funcs) injectfn() string {
	return f.inject
}

func (f *Funcs) contextfn() bool {
	return f.context != "disable"
}

func (f *Funcs) context_both() bool {
	return f.context == "both"
}

func (f *Funcs) context_disable() bool {
	return f.context == "disable"
}

// nameContext generates a name with the context suffix.
func nameContext(context bool, name string) string {
	if context {
		return name + "Context"
	}
	return name
}

// func_name_context generates a func name with context determined by the
// context mode.
func (f *Funcs) func_name_context(v any) string {
	switch x := v.(type) {
	case string:
		return nameContext(f.context_both(), x)
	case Query:
		return nameContext(f.context_both(), x.Name)
	case Table:
		return nameContext(f.context_both(), x.GoName)
	case ForeignKey:
		return nameContext(f.context_both(), x.GoName)
	case Proc:
		n := x.GoName
		if x.Overloaded {
			n = x.OverloadedName
		}
		return nameContext(f.context_both(), n)
	case Index:
		return nameContext(f.context_both(), x.Func)
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 2: %T ]]", v)
}

// func_name_none generates a func name without context.
func (f *Funcs) func_name_none(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case Query:
		return x.Name
	case Table:
		return x.GoName
	case ForeignKey:
		return x.GoName
	case Proc:
		if x.Overloaded {
			return x.OverloadedName
		}
		return x.GoName
	case Index:
		return x.Func
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 1: %T ]]", v)
}

// funcfn builds a func definition.
func (f *Funcs) funcfn(name string, context bool, v any) string {
	var p, r []string
	if context {
		p = append(p, "ctx context.Context")
	}
	p = append(p, "db DB")
	switch x := v.(type) {
	case Query:
		// params
		for _, z := range x.Params {
			p = append(p, fmt.Sprintf("%s %s", z.Name, z.Type))
		}
		// returns
		switch {
		case x.Exec:
			r = append(r, "pgconn.CommandTag")
		case x.Flat:
			for _, z := range x.Type.Fields {
				r = append(r, f.typefn(z.Type))
			}
		case x.One:
			r = append(r, "*"+x.Type.GoName)
		default:
			r = append(r, "[]*"+x.Type.GoName)
		}
	case Proc:
		// params
		p = append(p, f.params(x.Params, true))
		// returns
		if !x.Void {
			for _, ret := range x.Returns {
				r = append(r, f.typefn(ret.Type))
			}
		}
	case Index:
		// params
		p = append(p, f.params(x.Fields, true))
		// returns
		rt := "*" + x.Table.GoName
		if !x.IsUnique {
			rt = "[]" + rt
		}
		r = append(r, rt)
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 3: %T ]]", v)
	}
	r = append(r, "error")
	return fmt.Sprintf("func %s(%s) (%s)", name, strings.Join(p, ", "), strings.Join(r, ", "))
}

// func_context generates a func signature for v with context determined by the
// context mode.
func (f *Funcs) func_context(v any) string {
	return f.funcfn(f.func_name_context(v), f.contextfn(), v)
}

// func_none generates a func signature for v without context.
func (f *Funcs) func_none(v any) string {
	return f.funcfn(f.func_name_none(v), false, v)
}

// recv builds a receiver func definition.
func (f *Funcs) recv(name string, context bool, t Table, v any) string {
	short := f.short(t)
	var p, r []string
	// determine params and return type
	if context {
		p = append(p, "ctx context.Context")
	}
	p = append(p, "db DB")
	switch x := v.(type) {
	case ForeignKey:
		r = append(r, "*"+x.RefTable)
	}
	r = append(r, "error")
	return fmt.Sprintf("func (%s *%s) %s(%s) (%s)", short, t.GoName, name, strings.Join(p, ", "), strings.Join(r, ", "))
}

// recv_context builds a receiver func definition with context determined by
// the context mode.
func (f *Funcs) recv_context(typ any, v any) string {
	switch x := typ.(type) {
	case Table:
		return f.recv(f.func_name_context(v), f.contextfn(), x, v)
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 4: %T ]]", typ)
}

// recv_none builds a receiver func definition without context.
func (f *Funcs) recv_none(typ any, v any) string {
	switch x := typ.(type) {
	case Table:
		return f.recv(f.func_name_none(v), false, x, v)
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 5: %T ]]", typ)
}

func (f *Funcs) foreign_key_context(v any) string {
	var name string
	var p []string
	if f.contextfn() {
		p = append(p, "ctx")
	}
	switch x := v.(type) {
	case ForeignKey:
		name = x.RefFunc
		if f.context_both() {
			name += "Context"
		}
		// add params
		p = append(p, "db", f.convertTypes(x))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 6: %T ]]", v)
	}
	return fmt.Sprintf("%s(%s)", name, strings.Join(p, ", "))
}

func (f *Funcs) foreign_key_none(v any) string {
	var name string
	var p []string
	switch x := v.(type) {
	case ForeignKey:
		name = x.RefFunc
		p = append(p, "context.Background()", "db", f.convertTypes(x))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 7: %T ]]", v)
	}
	return fmt.Sprintf("%s(%s)", name, strings.Join(p, ", "))
}

// db generates a db.<n>(ctx, sqlstr, ...)
func (f *Funcs) db(name string, v ...any) string {
	// params
	var p []any
	if f.contextfn() {
		p = append(p, "ctx")
	}
	p = append(p, "sqlstr")
	return fmt.Sprintf("db.%s(%s)", name, f.names("", append(p, v...)...))
}

// db_prefix generates a db.<n>(ctx, sqlstr, <prefix>.param, ...).
func (f *Funcs) db_prefix(name string, skip bool, vs ...any) string {
	var prefix string
	var params []any
	for i, v := range vs {
		var ignore []string
		switch x := v.(type) {
		case string:
			params = append(params, x)
		case Table:
			prefix = f.short(x.GoName) + "."
			// skip primary keys
			if skip {
				for _, field := range x.Fields {
					if field.IsSequence {
						ignore = append(ignore, field.GoName)
					}
				}
			}
			p := f.names_ignore(prefix, v, ignore...)
			if p != "" {
				params = append(params, p)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 8 (%d): %T ]]", i, v)
		}
	}
	return f.db(name, params...)
}

// db_update generates a db.<n>(ctx, sqlstr, regularparams, primaryparams)
func (f *Funcs) db_update(name string, v any) string {
	var ignore, p []string
	switch x := v.(type) {
	case Table:
		prefix := f.short(x.GoName) + "."
		for _, pk := range x.PrimaryKeys {
			ignore = append(ignore, pk.GoName)
		}
		p = append(p, f.names_ignore(prefix, x, ignore...), f.names(prefix, x.PrimaryKeys))
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 9: %T ]]", v)
	}
	return f.db(name, strings.Join(p, ", "))
}

func (f *Funcs) logf_pkeys(v any) string {
	p := []string{"sqlstr"}
	switch x := v.(type) {
	case Table:
		if names := f.names(f.short(x.GoName)+".", x.PrimaryKeys); names != "" {
			p = append(p, names)
		}
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

func (f *Funcs) logf(v any, ignore ...any) string {
	var ignoreNames []string
	p := []string{"sqlstr"}
	// build ignore list
	for i, x := range ignore {
		switch z := x.(type) {
		case string:
			ignoreNames = append(ignoreNames, z)
		case Field:
			ignoreNames = append(ignoreNames, z.GoName)
		case []Field:
			for _, fld := range z {
				ignoreNames = append(ignoreNames, fld.GoName)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 11 (%d): %T ]]", i, x)
		}
	}
	// add fields
	switch x := v.(type) {
	case Table:
		if names := f.names_ignore(f.short(x.GoName)+".", x, ignoreNames...); names != "" {
			p = append(p, names)
		}
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 12: %T ]]", v)
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

func (f *Funcs) logf_update(v any) string {
	var ignore []string
	p := []string{"sqlstr"}
	switch x := v.(type) {
	case Table:
		prefix := f.short(x.GoName) + "."
		for _, pk := range x.PrimaryKeys {
			ignore = append(ignore, pk.GoName)
		}
		if names := f.names_ignore(prefix, x, ignore...); names != "" {
			p = append(p, names)
		}
		if names := f.names(prefix, x.PrimaryKeys); names != "" {
			p = append(p, names)
		}
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 13: %T ]]", v)
	}
	return fmt.Sprintf("logf(%s)", strings.Join(p, ", "))
}

// names generates a list of names.
func (f *Funcs) names(prefix string, z ...any) string {
	var names []string
	for i, v := range z {
		switch x := v.(type) {
		case string:
			names = append(names, x)
		case Query:
			for _, p := range x.Params {
				if p.Interpolate {
					continue
				}
				names = append(names, prefix+p.Name)
			}
		case Table:
			for _, field := range x.Fields {
				names = append(names, prefix+field.GoName)
			}
		case []Field:
			for _, field := range x {
				names = append(names, prefix+field.GoName)
			}
		case Index:
			for _, field := range x.Fields {
				names = append(names, prefix+field.GoName)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 14 (%d): %T ]]", i, v)
		}
	}
	return strings.Join(names, ", ")
}

// names_all is like names, but includes interpolate params.
func (f *Funcs) names_all(prefix string, z ...any) string {
	var names []string
	for i, v := range z {
		switch x := v.(type) {
		case string:
			names = append(names, x)
		case Query:
			for _, p := range x.Params {
				names = append(names, prefix+p.Name)
			}
		case Table:
			for _, field := range x.Fields {
				names = append(names, prefix+field.GoName)
			}
		case []Field:
			for _, field := range x {
				names = append(names, prefix+field.GoName)
			}
		case Index:
			for _, field := range x.Fields {
				names = append(names, prefix+field.GoName)
			}
		default:
			return fmt.Sprintf("[[ UNSUPPORTED TYPE 15 (%d): %T ]]", i, v)
		}
	}
	return strings.Join(names, ", ")
}

// names_ignore is like names, but ignores certain fields.
func (f *Funcs) names_ignore(prefix string, v any, ignore ...string) string {
	var names []string
	switch x := v.(type) {
	case Table:
		for _, field := range x.Fields {
			if slices.Contains(ignore, field.GoName) {
				continue
			}
			names = append(names, prefix+field.GoName)
		}
	case []Field:
		for _, field := range x {
			if slices.Contains(ignore, field.GoName) {
				continue
			}
			names = append(names, prefix+field.GoName)
		}
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 16: %T ]]", v)
	}
	return strings.Join(names, ", ")
}

// params generates a list of params.
func (f *Funcs) params(v any, named bool) string {
	var params []string
	switch x := v.(type) {
	case []Field:
		for _, field := range x {
			param := field.GoName
			if named {
				param = param + " " + field.Type
			}
			params = append(params, param)
		}
	}
	return strings.Join(params, ", ")
}

// zero generates the zero value for fields.
func (f *Funcs) zero(z ...any) string {
	var vals []string
	for _, v := range z {
		switch x := v.(type) {
		case string:
			vals = append(vals, x)
		case []Field:
			for _, field := range x {
				vals = append(vals, field.Zero)
			}
		}
	}
	return strings.Join(vals, ", ")
}

// typefn returns the type.
func (f *Funcs) typefn(v any) string {
	switch x := v.(type) {
	case string:
		return x
	}
	return fmt.Sprintf("%v", v)
}

// field generates a field definition.
func (f *Funcs) field(field Field) string {
	tag := ""
	if f.fieldtag != nil {
		buf := new(bytes.Buffer)
		if err := f.fieldtag.Execute(buf, field); err != nil {
			return fmt.Sprintf("[[ error: %v ]]", err)
		}
		tag = fmt.Sprintf(" `%s`", buf.String())
	}
	return fmt.Sprintf("%s %s%s // %s", field.GoName, field.Type, tag, field.SQLName)
}

// short generates the short name for a type.
func (f *Funcs) short(v any) string {
	var n string
	switch x := v.(type) {
	case string:
		n = x
	case Table:
		n = x.GoName
	case Index:
		n = x.Table.GoName
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 17: %T ]]", v)
	}
	// check short map
	if s, ok := f.shorts[n]; ok {
		return s
	}
	// generate a short name
	s := strings.ToLower(n[:1])
	// check conflicts
	for checkName(s) {
		s = s + s
	}
	f.shorts[n] = s
	return s
}

// querystr builds a query string.
func (f *Funcs) querystr(v any) string {
	switch x := v.(type) {
	case Query:
		return buildQuerystr(x)
	}
	return fmt.Sprintf("[[ UNSUPPORTED TYPE 18: %T ]]", v)
}

// buildQuerystr builds a query string for a Query.
func buildQuerystr(q Query) string {
	var lines []string
	for i, line := range q.Query {
		if i == 0 {
			lines = append(lines, fmt.Sprintf("const sqlstr = `%s` +", line))
		} else if i == len(q.Query)-1 {
			lines = append(lines, fmt.Sprintf("\t\t`%s`", line))
		} else {
			lines = append(lines, fmt.Sprintf("\t\t`%s` +", line))
		}
	}
	return strings.Join(lines, "\n")
}

// sqlstr generates a sql string.
func (f *Funcs) sqlstr(typ string, v any) string {
	var lines []string
	switch typ {
	case "insert":
		lines = f.sqlstr_insert(v)
	case "update":
		lines = f.sqlstr_update(v)
	case "upsert":
		lines = f.sqlstr_upsert(v)
	case "delete":
		lines = f.sqlstr_delete(v)
	case "index":
		lines = f.sqlstr_index(v)
	case "proc":
		lines = f.sqlstr_proc(v)
	default:
		return fmt.Sprintf("[[ UNSUPPORTED SQLSTR TYPE: %s ]]", typ)
	}
	return buildSqlstr(lines)
}

// buildSqlstr builds a sqlstr.
func buildSqlstr(lines []string) string {
	var res []string
	for i, line := range lines {
		if i == 0 {
			res = append(res, fmt.Sprintf("const sqlstr = `%s` +", line))
		} else if i == len(lines)-1 {
			res = append(res, fmt.Sprintf("\t\t`%s`", line))
		} else {
			res = append(res, fmt.Sprintf("\t\t`%s` +", line))
		}
	}
	return strings.Join(res, "\n")
}

// colname returns a column name.
func (f *Funcs) colname(field Field) string {
	if f.escColumn {
		return `"` + field.SQLName + `"`
	}
	return field.SQLName
}

// sqlstr_insert_base builds the base insert query.
func (f *Funcs) sqlstr_insert_base(all bool, t Table) []string {
	// build names and values
	var n int
	var fields, vals []string
	for _, field := range t.Fields {
		if field.IsSequence && !all {
			continue
		}
		fields = append(fields, f.colname(field))
		vals = append(vals, f.nth(n))
		n++
	}
	return []string{
		"INSERT INTO " + f.schemafn(t.SQLName) + " (",
		strings.Join(fields, ", ") + ") ",
		"VALUES (" + strings.Join(vals, ", ") + ")",
	}
}

// sqlstr_insert builds an insert query.
func (f *Funcs) sqlstr_insert(v any) []string {
	switch x := v.(type) {
	case Table:
		lines := f.sqlstr_insert_base(false, x)
		// add returning
		if f.hasSequence(x) {
			seq := f.seqField(x)
			lines = append(lines, " RETURNING "+f.colname(seq))
		}
		return lines
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 19: %T ]]", v)}
}

// sqlstr_update_base builds the base update query.
func (f *Funcs) sqlstr_update_base(prefix string, v any) (int, []string) {
	switch x := v.(type) {
	case Table:
		var n int
		var fields []string
		for _, field := range x.Fields {
			if field.IsPrimary {
				continue
			}
			val := f.nth(n)
			if prefix != "" {
				val = prefix + field.SQLName
			}
			fields = append(fields, f.colname(field)+" = "+val)
			n++
		}
		return n, []string{
			"UPDATE " + f.schemafn(x.SQLName) + " SET ",
			strings.Join(fields, ", ") + " ",
		}
	}
	return 0, []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 20: %T ]]", v)}
}

// sqlstr_update builds an update query.
func (f *Funcs) sqlstr_update(v any) []string {
	switch x := v.(type) {
	case Table:
		var list []string
		n, lines := f.sqlstr_update_base("", v)
		for i, z := range x.PrimaryKeys {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(n+i)))
		}
		return append(lines, "WHERE "+strings.Join(list, " AND "))
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 21: %T ]]", v)}
}

// sqlstr_upsert builds an upsert query.
func (f *Funcs) sqlstr_upsert(v any) []string {
	switch x := v.(type) {
	case Table:
		lines := f.sqlstr_insert_base(true, x)
		// build conflict clause
		var conflicts []string
		for _, pk := range x.PrimaryKeys {
			conflicts = append(conflicts, pk.SQLName)
		}
		lines = append(lines, " ON CONFLICT ("+strings.Join(conflicts, ", ")+") DO ")
		// build update clause
		_, update := f.sqlstr_update_base("EXCLUDED.", x)
		if len(update) > 1 {
			lines = append(lines, update[1:]...)
		}
		// add returning if has sequence
		if f.hasSequence(x) {
			seq := f.seqField(x)
			lines = append(lines, " RETURNING "+f.colname(seq))
		}
		return lines
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 22: %T ]]", v)}
}

// sqlstr_delete builds a delete query.
func (f *Funcs) sqlstr_delete(v any) []string {
	switch x := v.(type) {
	case Table:
		var list []string
		for i, z := range x.PrimaryKeys {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(i)))
		}
		return []string{
			"DELETE FROM " + f.schemafn(x.SQLName) + " ",
			"WHERE " + strings.Join(list, " AND "),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 23: %T ]]", v)}
}

// sqlstr_index builds an index query.
func (f *Funcs) sqlstr_index(v any) []string {
	switch x := v.(type) {
	case Index:
		// build table fields
		var fields []string
		for _, z := range x.Table.Fields {
			fields = append(fields, f.colname(z))
		}
		// build where clause
		var list []string
		for i, z := range x.Fields {
			list = append(list, fmt.Sprintf("%s = %s", f.colname(z), f.nth(i)))
		}
		return []string{
			"SELECT ",
			strings.Join(fields, ", ") + " ",
			"FROM " + f.schemafn(x.Table.SQLName) + " ",
			"WHERE " + strings.Join(list, " AND "),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 24: %T ]]", v)}
}

// sqlstr_proc builds a stored procedure call.
func (f *Funcs) sqlstr_proc(v any) []string {
	switch x := v.(type) {
	case Proc:
		if x.Type == "function" {
			return f.sqlstr_func(v)
		}
		// build params list
		var list []string
		for i := range x.Params {
			list = append(list, f.nth(i))
		}
		return []string{
			fmt.Sprintf("CALL %s(%s)", f.schemafn(x.SQLName), strings.Join(list, ", ")),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 25: %T ]]", v)}
}

// sqlstr_func builds a function call.
func (f *Funcs) sqlstr_func(v any) []string {
	switch x := v.(type) {
	case Proc:
		var list []string
		for i := range x.Params {
			list = append(list, f.nth(i))
		}
		return []string{
			fmt.Sprintf("SELECT * FROM %s(%s)", f.schemafn(x.SQLName), strings.Join(list, ", ")),
		}
	}
	return []string{fmt.Sprintf("[[ UNSUPPORTED TYPE 26: %T ]]", v)}
}

// hasSequence returns true if the table has a sequence field.
func (f *Funcs) hasSequence(t Table) bool {
	for _, field := range t.Fields {
		if field.IsSequence {
			return true
		}
	}
	return false
}

// seqField returns the sequence field.
func (f *Funcs) seqField(t Table) Field {
	for _, field := range t.Fields {
		if field.IsSequence {
			return field
		}
	}
	return Field{}
}

// convertTypes generates the conversions to convert the foreign key field
// types to their respective referenced field types.
func (f *Funcs) convertTypes(fkey ForeignKey) string {
	var p []string
	for i := range fkey.Fields {
		field := fkey.Fields[i]
		refField := fkey.RefFields[i]
		expr := f.short(fkey.Table) + "." + field.GoName
		if field.Type != refField.Type {
			expr = refField.Type + "(" + expr + ")"
		}
		p = append(p, expr)
	}
	return strings.Join(p, ", ")
}

// checkName checks if a name is a Go keyword or builtin.
func checkName(s string) bool {
	switch s {
	case "break", "case", "chan", "const", "continue", "default", "defer",
		"else", "fallthrough", "for", "func", "go", "goto", "if", "import",
		"interface", "map", "package", "range", "return", "select", "struct",
		"switch", "type", "var":
		return true
	case "append", "cap", "close", "complex", "copy", "delete", "imag", "len",
		"make", "new", "panic", "print", "println", "real", "recover":
		return true
	case "bool", "byte", "complex128", "complex64", "error", "float32",
		"float64", "int", "int16", "int32", "int64", "int8", "rune", "string",
		"uint", "uint16", "uint32", "uint64", "uint8", "uintptr":
		return true
	case "true", "false", "iota", "nil":
		return true
	case "ctx", "db", "err", "sqlstr", "res", "rows":
		return true
	}
	return false
}

// eval evaluates text with value replacement.
func eval(z ...any) string {
	if len(z) == 0 {
		return ""
	}
	var s string
	switch x := z[0].(type) {
	case string:
		s = x
	default:
		return fmt.Sprintf("[[ UNSUPPORTED TYPE 27: %T ]]", z[0])
	}
	if len(z) == 1 {
		return s
	}
	for i := 1; i < len(z); i++ {
		s = strings.ReplaceAll(s, fmt.Sprintf("%%[%d]", i), fmt.Sprintf("%v", z[i]))
	}
	return s
}
