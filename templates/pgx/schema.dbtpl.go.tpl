{{ define "enum" }}
{{- $e := .Data -}}
// {{ $e.GoName }} is the '{{ $e.SQLName }}' enum type from schema '{{ schema }}'.
type {{ $e.GoName }} string

// {{ $e.GoName }} values.
const (
{{ range $e.Values -}}
	// {{ $e.GoName }}{{ .GoName }} is the '{{ .SQLName }}' {{ $e.SQLName }}.
	{{ $e.GoName }}{{ .GoName }} {{ $e.GoName }} = "{{ .SQLName }}"
{{ end -}}
)

// String satisfies the [fmt.Stringer] interface.
func ({{ short $e.GoName }} {{ $e.GoName }}) String() string {
	return string({{ short $e.GoName }})
}

// Valid returns true if the value is a valid {{ $e.GoName }}.
func ({{ short $e.GoName }} {{ $e.GoName }}) Valid() bool {
	switch {{ short $e.GoName }} {
{{ range $e.Values -}}
	case {{ $e.GoName }}{{ .GoName }}:
		return true
{{ end -}}
	}
	return false
}

// Scan implements the [pgx] Scanner interface.
func ({{ short $e.GoName }} *{{ $e.GoName }}) Scan(v any) error {
	switch x := v.(type) {
	case []byte:
		*{{ short $e.GoName }} = {{ $e.GoName }}(x)
	case string:
		*{{ short $e.GoName }} = {{ $e.GoName }}(x)
	default:
		return fmt.Errorf("cannot scan type %T into {{ $e.GoName }}", v)
	}
	if !{{ short $e.GoName }}.Valid() {
		return ErrInvalid{{ $e.GoName }}(string(*{{ short $e.GoName }}))
	}
	return nil
}

{{ $nullName := (printf "%s%s" "Null" $e.GoName) -}}
{{- $nullShort := (short $nullName) -}}
// {{ $nullName }} represents a null '{{ $e.SQLName }}' enum for schema '{{ schema }}'.
type {{ $nullName }} struct {
	{{ $e.GoName }} {{ $e.GoName }}
	// Valid is true if [{{ $e.GoName }}] is not null.
	Valid bool
}

// Scan implements the [pgx] Scanner interface.
func ({{ $nullShort }} *{{ $nullName }}) Scan(v any) error {
	if v == nil {
		{{ $nullShort }}.{{ $e.GoName }}, {{ $nullShort }}.Valid = "", false
		return nil
	}
	err := {{ $nullShort }}.{{ $e.GoName }}.Scan(v)
	{{ $nullShort }}.Valid = err == nil
	return err
}

// ErrInvalid{{ $e.GoName }} is the invalid [{{ $e.GoName }}] error.
type ErrInvalid{{ $e.GoName }} string

// Error satisfies the error interface.
func (err ErrInvalid{{ $e.GoName }}) Error() string {
	return fmt.Sprintf("invalid {{ $e.GoName }}(%s)", string(err))
}
{{ end }}

{{ define "foreignkey" }}
{{- $k := .Data -}}
// {{ func_name_context $k }} returns the {{ $k.RefTable }} associated with the [{{ $k.Table.GoName }}]'s ({{ names "" $k.Fields }}).
//
// Generated from foreign key '{{ $k.SQLName }}'.
{{ recv_context $k.Table $k }} {
	return {{ foreign_key_context $k }}
}
{{- if context_both }}

// {{ func_name $k }} returns the {{ $k.RefTable }} associated with the {{ $k.Table }}'s ({{ names "" $k.Fields }}).
//
// Generated from foreign key '{{ $k.SQLName }}'.
{{ recv $k.Table $k }} {
	return {{ foreign_key $k }}
}
{{- end }}
{{ end }}

{{ define "index" }}
{{- $i := .Data -}}
// {{ func_name_context $i }} retrieves a row from '{{ schema $i.Table.SQLName }}' as a [{{ $i.Table.GoName }}].
//
// Generated from index '{{ $i.SQLName }}'.
{{ func_context $i }} {
	// query
	{{ sqlstr "index" $i }}
	// run
	logf(sqlstr, {{ params $i.Fields false }})
{{- if $i.IsUnique }}
	{{ short $i.Table }} := {{ $i.Table.GoName }}{
	{{- if $i.Table.PrimaryKeys }}
		_exists: true,
	{{ end -}}
	}
	if err := {{ db "QueryRow"  $i }}.Scan({{ names (print "&" (short $i.Table) ".") $i.Table }}); err != nil {
		return nil, logerror(err)
	}
	return &{{ short $i.Table }}, nil
{{- else }}
	rows, err := {{ db "Query" $i }}
	if err != nil {
		return nil, logerror(err)
	}
	defer rows.Close()
	// process
	var res []*{{ $i.Table.GoName }}
	for rows.Next() {
		{{ short $i.Table }} := {{ $i.Table.GoName }}{
		{{- if $i.Table.PrimaryKeys }}
			_exists: true,
		{{ end -}}
		}
		// scan
		if err := rows.Scan({{ names_ignore (print "&" (short $i.Table) ".")  $i.Table }}); err != nil {
			return nil, logerror(err)
		}
		res = append(res, &{{ short $i.Table }})
	}
	if err := rows.Err(); err != nil {
		return nil, logerror(err)
	}
	return res, nil
{{- end }}
}

{{ if context_both -}}
// {{ func_name $i }} retrieves a row from '{{ schema $i.Table.SQLName }}' as a [{{ $i.Table.GoName }}].
//
// Generated from index '{{ $i.SQLName }}'.
{{ func $i }} {
	return {{ func_name_context $i }}({{ names "" "context.Background()" "db" $i }})
}
{{- end }}

{{end}}

{{ define "procs" }}
{{- $ps := .Data -}}
{{- range $p := $ps -}}
// {{ func_name_context $p }} calls the stored {{ $p.Type }} '{{ $p.Signature }}' on db.
{{ func_context $p }} {
	// query
	{{ sqlstr "proc" $p }}
	// run
{{- if and $p.Void (not $p.Returns) }}
	logf(sqlstr)
	if _, err := {{ db "Exec" $p }}; err != nil {
		return logerror(err)
	}
	return nil
{{- else }}
{{- range $p.Returns }}
	var {{ .GoName }} {{ type .Type }}
{{- end }}
	logf(sqlstr, {{ params $p.Params false }})
	if err := {{ db "QueryRow" $p }}.Scan({{ names "&" $p.Returns }}); err != nil {
		return {{ zero $p.Returns "logerror(err)" }}
	}
	return {{ names "" $p.Returns "nil" }}
{{- end }}
}

{{ if context_both -}}
// {{ func_name $p }} calls the stored {{ $p.Type }} '{{ $p.Signature }}' on db.
{{ func $p }} {
	return {{ func_name_context $p }}({{ names_all "" "context.Background()" "db" $p }})
}
{{- end }}

{{ end -}}
{{ end }}

{{ define "typedef" }}
{{- $t := .Data -}}
{{- if $t.Comment -}}
// {{ $t.Comment | eval $t.GoName }}
{{- else -}}
// {{ $t.GoName }} represents a row from '{{ schema $t.SQLName }}'.
{{- end }}
type {{ $t.GoName }} struct {
{{ range $t.Fields -}}
    {{ field . }}
{{ end -}}
{{- if $t.PrimaryKeys }}
	// xo fields
	_exists bool
{{- end }}
}

{{ if $t.PrimaryKeys -}}
// Exists returns true when the [{{ $t.GoName }}] exists in the database.
func ({{ short $t }} *{{ $t.GoName }}) Exists() bool {
	return {{ short $t }}._exists
}

// Deleted returns true when the [{{ $t.GoName }}] has been marked for deletion
// from the database.
func ({{ short $t }} *{{ $t.GoName }}) Deleted() bool {
	return !{{ short $t }}._exists
}

// Insert inserts the [{{ $t.GoName }}] to the database.
{{ recv_context $t "Insert" }} {
	switch {
	case {{ short $t }}._exists: // already exists
		return logerror(&ErrInsertFailed{ErrAlreadyExists})
	}
{{ if $t.Manual -}}
	// insert (manual)
	{{ sqlstr "insert" $t }}
	// run
	{{ logf $t }}
	if _, err := {{ db_prefix "Exec" true $t }}; err != nil {
		return logerror(err)
	}
{{- else -}}
	// insert (primary key generated and target column is writable)
	{{ sqlstr "insert" $t }}
	// run
	{{ logf $t $t.PrimaryKeys }}
	if err := {{ db_prefix "QueryRow" true $t }}.Scan({{ names (print "&" (short $t) ".") $t.PrimaryKeys }}); err != nil {
		return logerror(err)
	}
{{- end }}
	// set exists
	{{ short $t }}._exists = true
	return nil
}
{{ if context_both }}
// Insert inserts the [{{ $t.GoName }}] to the database.
{{ recv $t "Insert" }} {
	return {{ short $t }}.InsertContext(context.Background(), db)
}
{{- end }}

// Update updates a [{{ $t.GoName }}] in the database.
{{ recv_context $t "Update" }} {
	switch {
	case !{{ short $t }}._exists: // doesn't exist
		return logerror(&ErrUpdateFailed{ErrDoesNotExist})
	}
	// update with primary key
	{{ sqlstr "update" $t }}
	// run
	{{ logf_update $t }}
	if _, err := {{ db_update "Exec" $t }}; err != nil {
		return logerror(err)
	}
	return nil
}
{{ if context_both }}
// Update updates a [{{ $t.GoName }}] in the database.
{{ recv $t "Update" }} {
	return {{ short $t }}.UpdateContext(context.Background(), db)
}
{{- end }}

// Save saves the [{{ $t.GoName }}] to the database.
{{ recv_context $t "Save" }} {
	if {{ short $t }}.Exists() {
		return {{ short $t }}.Update{{ if context_both }}Context{{ end }}({{ if not context_disable }}ctx, {{ end }}db)
	}
	return {{ short $t }}.Insert{{ if context_both }}Context{{ end }}({{ if not context_disable }}ctx, {{ end }}db)
}
{{ if context_both }}
// Save saves the [{{ $t.GoName }}] to the database.
{{ recv $t "Save" }} {
	return {{ short $t }}.SaveContext(context.Background(), db)
}
{{- end }}

// Upsert performs an upsert for [{{ $t.GoName }}].
{{ recv_context $t "Upsert" }} {
	// upsert
	{{ sqlstr "upsert" $t }}
	// run
	{{ logf $t }}
{{- if $t.Manual }}
	if _, err := {{ db_prefix "Exec" false $t }}; err != nil {
		return logerror(err)
	}
{{- else }}
	if err := {{ db_prefix "QueryRow" false $t }}.Scan({{ names (print "&" (short $t) ".") $t.PrimaryKeys }}); err != nil {
		return logerror(err)
	}
{{- end }}
	// set exists
	{{ short $t }}._exists = true
	return nil
}
{{ if context_both }}
// Upsert performs an upsert for [{{ $t.GoName }}].
{{ recv $t "Upsert" }} {
	return {{ short $t }}.UpsertContext(context.Background(), db)
}
{{- end }}

// Delete deletes the [{{ $t.GoName }}] from the database.
{{ recv_context $t "Delete" }} {
	switch {
	case !{{ short $t }}._exists: // doesn't exist
		return nil
	}
	// delete with primary key
	{{ sqlstr "delete" $t }}
	// run
	{{ logf_pkeys $t }}
	if _, err := {{ db "Exec" $t.PrimaryKeys }}; err != nil {
		return logerror(err)
	}
	// set deleted
	{{ short $t }}._exists = false
	return nil
}
{{ if context_both }}
// Delete deletes the [{{ $t.GoName }}] from the database.
{{ recv $t "Delete" }} {
	return {{ short $t }}.DeleteContext(context.Background(), db)
}
{{- end }}
{{- end }}

{{ end }}
