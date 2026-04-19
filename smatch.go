package smatch

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/vapstack/qx"
)

type MatchFunc func(v any) (bool, error)

type Matcher struct {
	recMap  map[string]*fieldRec
	srcType reflect.Type
	df      []diffField
}

type diffUnsafeEqFunc func(p1, p2 unsafe.Pointer) bool

type diffField struct {
	name     string
	index    []int
	sf       reflect.StructField
	dbName   string
	jsonName string
	fastEq   diffUnsafeEqFunc
}

// NewFor creates a Matcher for the struct type T.
// T may be a struct or a pointer to a struct (any pointer depth is allowed).
// Reflection data for the type is computed once and cached for reuse.
// The matcher supports addressing fields by Go name and by selected struct tag aliases.
func NewFor[T any]() (*Matcher, error) {
	var v T
	t := reflect.TypeOf(v)
	if t == nil {
		t = reflect.TypeOf((*T)(nil)).Elem()
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct: %v", t)
	}
	return typeMatcher(t)
}

// New creates a matcher for the type of the provided value.
// Pointer types allow faster matching.
func New(v any) (*Matcher, error) {
	if v == nil {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct, got nil")
	}
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("value must be a struct or pointer to a struct, got %v", t)
	}
	return typeMatcher(t)
}

// CompileFor is a convenience helper equivalent to NewFor[T]().Compile(expr).
// The resulting predicate accepts both T and *T values (including via interfaces).
func CompileFor[T any](expr qx.Expr) (MatchFunc, error) {
	m, err := NewFor[T]()
	if err != nil {
		return nil, err
	}
	return m.Compile(expr)
}

// Match evaluates the expression exp against v in one shot.
// v may be provided either as a struct value (T) or as a pointer to a struct (*T);
// interfaces wrapping T or *T are also supported.
// Nil values never match and return (false, nil).
// For repeated evaluations over the same type, prefer creating a Matcher via NewFor and reusing it.
func Match(v any, exp qx.Expr) (bool, error) {
	if v == nil {
		return false, nil
	}
	t := reflect.TypeOf(v)

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false, fmt.Errorf("value must be a struct or pointer to a struct: %v", t)
	}

	m, err := typeMatcher(t)
	if err != nil {
		return false, err
	}
	fn, err := m.Compile(exp)
	if err != nil {
		return false, err
	}
	return fn(v)
}

func typeMatcher(rt reflect.Type) (*Matcher, error) {
	m, err := getRecMap(rt)
	if err != nil {
		return nil, err
	}
	return &Matcher{
		recMap:  m,
		srcType: rt,
		df:      getDiffFields(rt),
	}, nil
}

type matchFunc func(ptr unsafe.Pointer, root reflect.Value) (bool, error)
type typeEqFunc func(v1, v2 reflect.Value) bool

type fieldRec struct {
	index []int
	depth int
	kind  fieldRecKind
}

type fieldRecKind uint8

const (
	fieldRecByFieldName fieldRecKind = iota
	fieldRecByAnonymousName
	fieldRecByJSONTag
	fieldRecByDBTag
)

var recMapCache sync.Map

func getRecMap(rt reflect.Type) (map[string]*fieldRec, error) {
	if rmap, exists := recMapCache.Load(rt); exists {
		return rmap.(map[string]*fieldRec), nil
	}
	recMap := make(map[string]*fieldRec)
	if err := collectFieldRecs(rt, recMap, nil, 0); err != nil {
		return nil, err
	}
	recMapCache.Store(rt, recMap)
	return recMap, nil
}

func collectFieldRecs(rt reflect.Type, recMap map[string]*fieldRec, pos []int, depth int) error {
	nf := rt.NumField()
	for i := 0; i < nf; i++ {
		rf := rt.Field(i)

		if !rf.IsExported() && !rf.Anonymous {
			continue
		}

		nextPos := make([]int, 0, len(pos)+len(rf.Index))
		nextPos = append(nextPos, pos...)
		nextPos = append(nextPos, rf.Index...)

		if rf.IsExported() {
			nameKind := fieldRecByFieldName
			if rf.Anonymous {
				nameKind = fieldRecByAnonymousName
			}
			if err := registerFieldRec(recMap, rf.Name, &fieldRec{index: nextPos, depth: depth, kind: nameKind}, rf); err != nil {
				return err
			}

			if jsonTag := rf.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
				name := strings.TrimSpace(strings.Split(jsonTag, ",")[0])
				if name != "" {
					if err := registerFieldRec(recMap, name, &fieldRec{index: nextPos, depth: depth, kind: fieldRecByJSONTag}, rf); err != nil {
						return err
					}
				}
			}
			if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
				name := strings.TrimSpace(strings.Split(dbTag, ",")[0])
				if name != "" {
					if err := registerFieldRec(recMap, name, &fieldRec{index: nextPos, depth: depth, kind: fieldRecByDBTag}, rf); err != nil {
						return err
					}
				}
			}
		}

		if rf.Anonymous {
			ft := rf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				if err := collectFieldRecs(ft, recMap, nextPos, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func registerFieldRec(recMap map[string]*fieldRec, name string, rec *fieldRec, rf reflect.StructField) error {
	if existing, exists := recMap[name]; exists {
		if sameIndex(existing.index, rec.index) {
			return nil
		}
		if existing.kind == fieldRecByAnonymousName &&
			rec.kind != fieldRecByAnonymousName &&
			isStrictIndexPrefix(existing.index, rec.index) {
			recMap[name] = rec
			return nil
		}
		if rec.kind == fieldRecByAnonymousName &&
			existing.kind != fieldRecByAnonymousName &&
			isStrictIndexPrefix(rec.index, existing.index) {
			return nil
		}
		if rec.depth < existing.depth {
			recMap[name] = rec
			return nil
		}
		if rec.depth > existing.depth {
			return nil
		}

		switch rec.kind {
		case fieldRecByJSONTag, fieldRecByDBTag:
			return fmt.Errorf("duplicate field identifier \"%v\" while parsing %v", name, rf.Name)
		default:
			return fmt.Errorf("duplicate field identifier \"%v\", field name was taken by db/json tag", name)
		}
	}

	recMap[name] = rec
	return nil
}

func sameIndex(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func isStrictIndexPrefix(prefix, full []int) bool {
	if len(prefix) >= len(full) {
		return false
	}
	for i := range prefix {
		if prefix[i] != full[i] {
			return false
		}
	}
	return true
}

var diffFieldsCache sync.Map

func getDiffFields(rt reflect.Type) []diffField {
	if v, ok := diffFieldsCache.Load(rt); ok {
		return v.([]diffField)
	}
	var out []diffField
	collectDiffFields(rt, nil, &out)
	for i := range out {
		if off, leaf, ok := calcFastOffset(rt, out[i].index); ok && leaf == out[i].sf.Type {
			out[i].fastEq = buildDiffFastEq(off, leaf)
		}
	}
	diffFieldsCache.Store(rt, out)
	return out
}

func firstTagValue(tag string) string {
	if i := strings.IndexByte(tag, ','); i >= 0 {
		tag = tag[:i]
	}
	return strings.TrimSpace(tag)
}

func collectDiffFields(rt reflect.Type, pos []int, out *[]diffField) {
	nf := rt.NumField()
	for i := 0; i < nf; i++ {
		rf := rt.Field(i)

		if !rf.IsExported() && !rf.Anonymous {
			continue
		}

		nextPos := make([]int, 0, len(pos)+len(rf.Index))
		nextPos = append(nextPos, pos...)
		nextPos = append(nextPos, rf.Index...)

		if rf.IsExported() {
			df := diffField{
				name:  rf.Name,
				index: nextPos,
				sf:    rf,
			}

			if tv := firstTagValue(rf.Tag.Get("json")); tv != "" && tv != "-" {
				df.jsonName = tv
			}
			if tv := firstTagValue(rf.Tag.Get("db")); tv != "" && tv != "-" {
				df.dbName = tv
			}

			*out = append(*out, df)
		}

		if rf.Anonymous {
			ft := rf.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				collectDiffFields(ft, nextPos, out)
			}
		}
	}
}

func (m *Matcher) normalizeRoot(v any) (reflect.Value, error) {
	if v == nil {
		return reflect.Value{}, fmt.Errorf("nil value")
	}

	rv := reflect.ValueOf(v)

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil pointer")
		}
		rv = rv.Elem()
	}

	for rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil interface")
		}
		rv = rv.Elem()
		for rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				return reflect.Value{}, fmt.Errorf("nil pointer")
			}
			rv = rv.Elem()
		}
	}

	if rv.Type() != m.srcType {
		if rv.Type().ConvertibleTo(m.srcType) {
			rv = rv.Convert(m.srcType)
		} else {
			return reflect.Value{}, fmt.Errorf("type mismatch, expected %v, got %v", m.srcType, rv.Type())
		}
	}

	return rv, nil
}

// DiffFields returns the names of exported fields whose values differ
// between the provided values.
// Each value may be provided either as T or *T; interfaces wrapping T or *T
// are also supported.
// At least two values must be provided; if fewer are provided, it returns nil, nil.
// Values must be of the matcher's source type or convertible to it.
func (m *Matcher) DiffFields(values ...any) ([]string, error) {
	return m.DiffFieldsTag("", values...)
}

// DiffFieldsTag is like DiffFields but returns field names using the provided struct tag.
// Each value may be provided either as T or *T; interfaces wrapping T or *T
// are also supported.
// For each differing field, the first component of the tag (before a comma) is used;
// if the tag is missing or "-", it falls back to the Go field name.
// If tag is empty, DiffFieldsTag behaves the same as DiffFields.
func (m *Matcher) DiffFieldsTag(tag string, values ...any) ([]string, error) {
	if len(values) < 2 {
		return nil, nil
	}

	// fast
	if len(values) == 2 {
		rLeft, err := m.normalizeRoot(values[0])
		if err != nil {
			return nil, err
		}
		rRight, err := m.normalizeRoot(values[1])
		if err != nil {
			return nil, err
		}

		var pLeft, pRight unsafe.Pointer
		if rLeft.CanAddr() {
			pLeft = unsafe.Pointer(rLeft.UnsafeAddr())
		}
		if rRight.CanAddr() {
			pRight = unsafe.Pointer(rRight.UnsafeAddr())
		}

		out := make([]string, 0, len(m.df))

		for _, f := range m.df {
			if pLeft != nil && pRight != nil && f.fastEq != nil {
				if f.fastEq(pLeft, pRight) {
					continue
				}
				if name, ok := f.nameFor(tag, m.recMap); ok {
					out = append(out, name)
				}
				continue
			}

			vLeft, ok := getSafeField(rLeft, f.index)
			if !ok {
				vLeft = reflect.Value{}
			}
			vRight, ok := getSafeField(rRight, f.index)
			if !ok {
				vRight = reflect.Value{}
			}

			if areEqual(vLeft, vRight) {
				continue
			}

			if name, ok := f.nameFor(tag, m.recMap); ok {
				out = append(out, name)
			}
		}

		return out, nil
	}

	// general

	roots := make([]reflect.Value, len(values))
	for i, v := range values {
		rv, err := m.normalizeRoot(v)
		if err != nil {
			return nil, err
		}
		roots[i] = rv
	}

	base := roots[0]
	out := make([]string, 0, len(m.df))

	for _, f := range m.df {
		vBase, ok := getSafeField(base, f.index)
		if !ok {
			vBase = reflect.Value{}
		}

		diff := false
		for i := 1; i < len(roots); i++ {
			v, vok := getSafeField(roots[i], f.index)
			if !vok {
				v = reflect.Value{}
			}
			if !areEqual(vBase, v) {
				diff = true
				break
			}
		}
		if !diff {
			continue
		}

		if name, ok := f.nameFor(tag, m.recMap); ok {
			out = append(out, name)
		}
	}

	return out, nil
}

func (f diffField) nameFor(tag string, recMap map[string]*fieldRec) (string, bool) {
	if tag == "" {
		return diffFieldNameVisible(f.name, f.index, recMap)
	}

	switch tag {
	case "json":
		if f.jsonName != "" {
			if name, ok := diffFieldNameVisible(f.jsonName, f.index, recMap); ok {
				return name, true
			}
		}
	case "db":
		if f.dbName != "" {
			if name, ok := diffFieldNameVisible(f.dbName, f.index, recMap); ok {
				return name, true
			}
		}
	default:
		if tv := firstTagValue(f.sf.Tag.Get(tag)); tv != "" && tv != "-" {
			return tv, true
		}
	}

	return diffFieldNameVisible(f.name, f.index, recMap)
}

func diffFieldNameVisible(name string, index []int, recMap map[string]*fieldRec) (string, bool) {
	rec, ok := recMap[name]
	if !ok || !sameIndex(rec.index, index) {
		return "", false
	}
	return name, true
}

// Match evaluates the expression expr against v using this Matcher.
// v may be provided either as a struct value (T) or as a pointer to a struct (*T);
// interfaces wrapping T or *T are also supported.
// This method is intended for occasional, one-off evaluations.
// For repeated matching with the same expression, use Compile to obtain
// a reusable predicate.
func (m *Matcher) Match(v any, expr qx.Expr) (bool, error) {
	check, err := m.Compile(expr)
	if err != nil {
		return false, err
	}
	return check(v)
}

// Compile compiles expr into an efficient predicate function.
// The returned function may be called with either a struct value (T)
// or a pointer to a struct (*T); interfaces wrapping T or *T are also supported.
// Passing nil to the predicate returns (false, nil).
// The compiled predicate can be reused safely for repeated evaluations.
func (m *Matcher) Compile(expr qx.Expr) (MatchFunc, error) {

	check, err := m.compileRecursive(expr)
	if err != nil {
		return nil, err
	}

	return func(v any) (bool, error) {
		if v == nil {
			return false, nil
		}

		rv := reflect.ValueOf(v)

		var ptr unsafe.Pointer

		if rv.Kind() == reflect.Pointer {

			for rv.Kind() == reflect.Pointer {
				if rv.IsNil() {
					return false, nil
				}
				ptr = unsafe.Pointer(rv.Pointer())
				rv = rv.Elem()
			}
		}

		for rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return false, nil
			}
			rv = rv.Elem()

			if rv.Kind() == reflect.Pointer {
				for rv.Kind() == reflect.Pointer {
					if rv.IsNil() {
						return false, nil
					}
					ptr = unsafe.Pointer(rv.Pointer())
					rv = rv.Elem()
				}
			} else {
				ptr = nil
			}
		}

		if rv.Type() != m.srcType {
			if rv.Type().ConvertibleTo(m.srcType) {
				rv = rv.Convert(m.srcType)
				ptr = nil
			} else {
				return false, fmt.Errorf("type mismatch, expected %v, got %v", m.srcType, rv.Type())
			}
		} else if ptr == nil && rv.CanAddr() {
			ptr = unsafe.Pointer(rv.UnsafeAddr())
		}

		return check(ptr, rv)

	}, nil
}

func (m *Matcher) compileRecursive(expr qx.Expr) (matchFunc, error) {
	if expr.IsZero() {
		return nil, fmt.Errorf("empty expression")
	}
	if expr.Kind != qx.KindOP {
		return nil, fmt.Errorf("unsupported expression kind: %q", expr.Kind)
	}

	switch qx.Op(expr.Name) {
	case qx.OpAND, qx.OpOR:
		return m.compileLogical(qx.Op(expr.Name), expr.Args)

	case qx.OpNOT:
		if len(expr.Args) != 1 {
			return nil, fmt.Errorf("%s expects exactly 1 argument, got %d", expr.Name, len(expr.Args))
		}
		checker, err := m.compileRecursive(expr.Args[0])
		if err != nil {
			return nil, err
		}
		return negateMatchFunc(checker), nil

	case qx.OpEQ, qx.OpNE, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpIN, qx.OpHASALL, qx.OpHASANY, qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		return m.compilePredicate(expr)

	default:
		return nil, fmt.Errorf("unsupported op: %s", expr.Name)
	}
}

func (m *Matcher) compileLogical(op qx.Op, args []qx.Expr) (matchFunc, error) {
	var checker matchFunc

	switch len(args) {
	case 0:
		if op == qx.OpAND {
			checker = func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return true, nil }
		} else {
			checker = func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return false, nil }
		}

	case 1:
		return m.compileRecursive(args[0])

	case 2:
		left, err := m.compileRecursive(args[0])
		if err != nil {
			return nil, err
		}
		right, err := m.compileRecursive(args[1])
		if err != nil {
			return nil, err
		}

		if op == qx.OpAND {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				ok, err := left(ptr, root)
				if err != nil || !ok {
					return ok, err
				}
				return right(ptr, root)
			}
		} else {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				ok, err := left(ptr, root)
				if err != nil || ok {
					return ok, err
				}
				return right(ptr, root)
			}
		}

	case 3:
		left, err := m.compileRecursive(args[0])
		if err != nil {
			return nil, err
		}
		mid, err := m.compileRecursive(args[1])
		if err != nil {
			return nil, err
		}
		right, err := m.compileRecursive(args[2])
		if err != nil {
			return nil, err
		}

		if op == qx.OpAND {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				ok, err := left(ptr, root)
				if err != nil || !ok {
					return ok, err
				}
				ok, err = mid(ptr, root)
				if err != nil || !ok {
					return ok, err
				}
				return right(ptr, root)
			}
		} else {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				ok, err := left(ptr, root)
				if err != nil || ok {
					return ok, err
				}
				ok, err = mid(ptr, root)
				if err != nil || ok {
					return ok, err
				}
				return right(ptr, root)
			}
		}

	default:
		checks := make([]matchFunc, 0, len(args))
		for _, sub := range args {
			c, err := m.compileRecursive(sub)
			if err != nil {
				return nil, err
			}
			checks = append(checks, c)
		}

		if op == qx.OpAND {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				for _, check := range checks {
					ok, err := check(ptr, root)
					if err != nil {
						return false, err
					}
					if !ok {
						return false, nil
					}
				}
				return true, nil
			}
		} else {
			checker = func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
				for _, check := range checks {
					ok, err := check(ptr, root)
					if err != nil {
						return false, err
					}
					if ok {
						return true, nil
					}
				}
				return false, nil
			}
		}
	}

	return checker, nil
}

func (m *Matcher) compilePredicate(expr qx.Expr) (matchFunc, error) {
	if len(expr.Args) != 2 {
		return nil, fmt.Errorf("%s expects exactly 2 arguments, got %d", expr.Name, len(expr.Args))
	}

	left := expr.Args[0]
	right := expr.Args[1]
	if left.Kind != qx.KindREF {
		return nil, fmt.Errorf("unsupported left argument for %s: expected REF", expr.Name)
	}
	if right.Kind != qx.KindLIT {
		return nil, fmt.Errorf("unsupported right argument for %s: expected LIT", expr.Name)
	}

	fieldName := strings.TrimSpace(left.Name)
	if fieldName == "" {
		return nil, fmt.Errorf("invalid field reference for %s", expr.Name)
	}

	rec, ok := m.recMap[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field: %s", fieldName)
	}
	fieldType := m.srcType.FieldByIndex(rec.index).Type

	op := qx.Op(expr.Name)
	negate := false
	if op == qx.OpNE {
		op = qx.OpEQ
		negate = true
	}

	var (
		checker matchFunc
		err     error
	)

	switch op {
	case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		checker, err = compileScalarHybrid(op, rec.index, m.srcType, fieldType, right.Value)

	case qx.OpIN, qx.OpHASALL, qx.OpHASANY:
		checker, err = compileSliceOp(op, rec.index, m.srcType, fieldType, right.Value)

	default:
		return nil, fmt.Errorf("unsupported op: %s", expr.Name)
	}

	if err != nil {
		return nil, fmt.Errorf("compile error for field '%s': %w", fieldName, err)
	}
	if negate {
		return negateMatchFunc(checker), nil
	}
	return checker, nil
}

func negateMatchFunc(original matchFunc) matchFunc {
	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		ok, err := original(ptr, root)
		return !ok, err
	}
}

func compileScalarHybrid(op qx.Op, index []int, rootType, fieldType reflect.Type, queryVal any) (matchFunc, error) {

	slow, err := compileScalarCmpSlow(op, index, fieldType, queryVal)
	if err != nil {
		return nil, err
	}

	off, leafType, ok := calcFastOffset(rootType, index)
	if !ok {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, nil
	}

	fast, err := compileScalarCmpFast(op, off, leafType, queryVal)
	if err != nil {
		// return at least slow
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, nil
	}

	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		if ptr != nil {
			return fast(ptr)
		}
		return slow(root)
	}, nil
}

func compileScalarCmpSlow(op qx.Op, index []int, fieldType reflect.Type, queryVal any) (func(reflect.Value) (bool, error), error) {

	if op == qx.OpEQ && isNilLikeQueryValue(queryVal) {
		return func(v reflect.Value) (bool, error) {
			fv, ok := getSafeField(v, index)
			if !ok {
				return isNilableType(fieldType), nil
			}
			for fv.Kind() == reflect.Interface {
				if fv.IsNil() {
					return true, nil
				}
				fv = fv.Elem()
			}
			return isNilableAndNil(fv), nil
		}, nil
	}

	// string operations on interface fields
	if fieldType.Kind() == reflect.Interface && (op == qx.OpPREFIX || op == qx.OpSUFFIX || op == qx.OpCONTAINS) {
		qv, err := prepareValue(reflect.TypeOf(""), queryVal)
		if err != nil {
			return nil, err
		}
		q := qv.String()

		return func(root reflect.Value) (bool, error) {
			fv, ok := getSafeField(root, index)
			if !ok {
				return false, nil
			}
			for fv.Kind() == reflect.Interface {
				if fv.IsNil() {
					return false, nil
				}
				fv = fv.Elem()
			}
			if fv.Kind() == reflect.Pointer {
				if fv.IsNil() {
					return false, nil
				}
				fv = fv.Elem()
			}
			if fv.Kind() != reflect.String {
				return false, nil
			}
			s := fv.String()
			switch op {
			case qx.OpPREFIX:
				return strings.HasPrefix(s, q), nil
			case qx.OpSUFFIX:
				return strings.HasSuffix(s, q), nil
			case qx.OpCONTAINS:
				return strings.Contains(s, q), nil
			default:
				return false, nil
			}
		}, nil
	}

	if fieldType.Kind() == reflect.Interface {
		if op == qx.OpEQ {
			if qNum, ok := numericValueFromRaw(queryVal); ok {
				qv, err := prepareValue(fieldType, queryVal)
				if err != nil {
					return nil, err
				}
				return func(root reflect.Value) (bool, error) {
					fv, ok := getSafeField(root, index)
					if !ok {
						return false, nil
					}
					nv := fv
					for nv.Kind() == reflect.Interface {
						if nv.IsNil() {
							return false, nil
						}
						nv = nv.Elem()
					}
					for nv.Kind() == reflect.Pointer {
						if nv.IsNil() {
							return false, nil
						}
						nv = nv.Elem()
					}
					if lv, ok := numericValueFromReflect(nv); ok {
						cmp, ordered := compareNumericValues(lv, qNum)
						return ordered && cmp == 0, nil
					}
					return areEqual(fv, qv), nil
				}, nil
			}
		}
		switch op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
			return compileInterfaceOrderedCmpSlow(op, index, queryVal), nil
		}
	}

	isPtrField := fieldType.Kind() == reflect.Pointer

	cmpType := fieldType
	if isPtrField {
		cmpType = fieldType.Elem()
	}

	if numeric, ok, err := compileNumericCrossCmpSlow(op, index, fieldType, queryVal); ok || err != nil {
		return numeric, err
	}

	qv, err := prepareValue(cmpType, queryVal)
	if err != nil {
		return nil, err
	}

	getVal := func(root reflect.Value) (reflect.Value, bool) {
		fv, ok := getSafeField(root, index)
		if !ok {
			return reflect.Value{}, false
		}
		for fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				return reflect.Value{}, false
			}
			fv = fv.Elem()
		}
		if isPtrField {
			if fv.Kind() != reflect.Pointer || fv.IsNil() {
				return reflect.Value{}, false
			}
			fv = fv.Elem()
		}
		return fv, true
	}

	switch cmpType.Kind() {

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		q := qv.Int()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedInt(op, val.Int(), q), nil
		}, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		q := qv.Uint()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedUint(op, val.Uint(), q), nil
		}, nil

	case reflect.Float32, reflect.Float64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		q := qv.Float()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return cmpOrderedFloat(op, val.Float(), q), nil
		}, nil

	case reflect.String:
		q := qv.String()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			lv := val.String()
			switch op {
			case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
				return cmpOrderedString(op, lv, q), nil
			case qx.OpPREFIX:
				return strings.HasPrefix(lv, q), nil
			case qx.OpSUFFIX:
				return strings.HasSuffix(lv, q), nil
			case qx.OpCONTAINS:
				return strings.Contains(lv, q), nil
			default:
				return false, fmt.Errorf("operation %v is not supported for string", op)
			}
		}, nil

	case reflect.Bool:
		if op != qx.OpEQ {
			return nil, fmt.Errorf("operation %v is not supported for bool", op)
		}
		q := qv.Bool()
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return val.Bool() == q, nil
		}, nil
	}

	// fallback
	if op == qx.OpEQ {
		eqFast, hasFast := getTypeEqFunc(cmpType)
		useEqFast := hasFast && qv.IsValid() && qv.Type() == cmpType
		if useEqFast {
			return func(root reflect.Value) (bool, error) {
				val, ok := getVal(root)
				if !ok {
					return false, nil
				}
				return eqFast(val, qv), nil
			}, nil
		}
		return func(root reflect.Value) (bool, error) {
			val, ok := getVal(root)
			if !ok {
				return false, nil
			}
			return areEqual(val, qv), nil
		}, nil
	}

	return nil, fmt.Errorf("unsupported type %v", cmpType)
}

func compileInterfaceOrderedCmpSlow(op qx.Op, index []int, queryVal any) func(reflect.Value) (bool, error) {
	qNum, hasNumeric := numericValueFromRaw(queryVal)

	return func(root reflect.Value) (bool, error) {
		fv, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}
		for fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}

		if hasNumeric {
			if lv, ok := numericValueFromReflect(fv); ok {
				cmp, ordered := compareNumericValues(lv, qNum)
				if !ordered {
					return false, nil
				}
				return matchOrderedCmp(op, cmp), nil
			}
		}

		qv, err := prepareValue(fv.Type(), queryVal)
		if err != nil {
			return false, err
		}

		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return cmpOrderedInt(op, fv.Int(), qv.Int()), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return cmpOrderedUint(op, fv.Uint(), qv.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return cmpOrderedFloat(op, fv.Float(), qv.Float()), nil
		case reflect.String:
			return cmpOrderedString(op, fv.String(), qv.String()), nil
		default:
			return false, nil
		}
	}
}

func compileScalarCmpFast(op qx.Op, off uintptr, leafType reflect.Type, queryVal any) (func(unsafe.Pointer) (bool, error), error) {

	if op == qx.OpEQ && isNilLikeQueryValue(queryVal) {

		if leafType.Kind() == reflect.Pointer {
			return func(ptr unsafe.Pointer) (bool, error) {
				p := *(*unsafe.Pointer)(unsafe.Add(ptr, off))
				return p == nil, nil
			}, nil
		}

		return nil, fmt.Errorf("fast nil EQ is not supported for %v", leafType)
	}

	isPtrField := leafType.Kind() == reflect.Pointer
	cmpType := leafType
	if isPtrField {
		cmpType = leafType.Elem()
	}

	if numeric, ok, err := compileNumericCrossCmpFast(op, off, leafType, queryVal); ok || err != nil {
		return numeric, err
	}

	qv, err := prepareValue(cmpType, queryVal)
	if err != nil {
		return nil, err
	}

	switch cmpType.Kind() {

	case reflect.Int:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastInt[int](op, off, qv.Int(), isPtrField), nil
	case reflect.Int8:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastInt[int8](op, off, qv.Int(), isPtrField), nil
	case reflect.Int16:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastInt[int16](op, off, qv.Int(), isPtrField), nil
	case reflect.Int32:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastInt[int32](op, off, qv.Int(), isPtrField), nil
	case reflect.Int64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastInt[int64](op, off, qv.Int(), isPtrField), nil

	case reflect.Uint:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastUint[uint](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint8:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastUint[uint8](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint16:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastUint[uint16](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint32:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastUint[uint32](op, off, qv.Uint(), isPtrField), nil
	case reflect.Uint64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastUint[uint64](op, off, qv.Uint(), isPtrField), nil

	case reflect.Float32:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastFloat[float32](op, off, qv.Float(), isPtrField), nil
	case reflect.Float64:
		if !isOrderedOp(op) {
			return nil, fmt.Errorf("operation %v is not supported for %v", op, cmpType)
		}
		return makeFastFloat[float64](op, off, qv.Float(), isPtrField), nil

	case reflect.String:
		q := qv.String()
		if !isPtrField {
			return func(ptr unsafe.Pointer) (bool, error) {
				lv := *(*string)(unsafe.Add(ptr, off))
				switch op {
				case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
					return cmpOrderedString(op, lv, q), nil
				case qx.OpPREFIX:
					return strings.HasPrefix(lv, q), nil
				case qx.OpSUFFIX:
					return strings.HasSuffix(lv, q), nil
				case qx.OpCONTAINS:
					return strings.Contains(lv, q), nil
				default:
					return false, fmt.Errorf("operation %v is not supported for string", op)
				}
			}, nil
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			switch op {
			case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
				return cmpOrderedString(op, *p, q), nil
			case qx.OpPREFIX:
				return strings.HasPrefix(*p, q), nil
			case qx.OpSUFFIX:
				return strings.HasSuffix(*p, q), nil
			case qx.OpCONTAINS:
				return strings.Contains(*p, q), nil
			default:
				return false, fmt.Errorf("operation %v is not supported for string", op)
			}
		}, nil

	case reflect.Bool:
		if op != qx.OpEQ {
			return nil, fmt.Errorf("operation %v is not supported for bool", op)
		}
		q := qv.Bool()
		if !isPtrField {
			return func(ptr unsafe.Pointer) (bool, error) {
				lv := *(*bool)(unsafe.Add(ptr, off))
				return lv == q, nil
			}, nil
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**bool)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			return *p == q, nil
		}, nil
	}

	return nil, fmt.Errorf("fast path unsupported for %v", cmpType)
}

func makeFastInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](op qx.Op, off uintptr, qVal int64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedInt(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedInt(op, *p, q), nil
	}
}

func makeFastUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](op qx.Op, off uintptr, qVal uint64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedUint(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedUint(op, *p, q), nil
	}
}

func makeFastFloat[T ~float32 | ~float64](op qx.Op, off uintptr, qVal float64, isPtr bool) func(unsafe.Pointer) (bool, error) {
	q := T(qVal)
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := *(*T)(unsafe.Add(ptr, off))
			return cmpOrderedFloat(op, lv, q), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return cmpOrderedFloat(op, *p, q), nil
	}
}

func compileNumericCrossCmpSlow(op qx.Op, index []int, fieldType reflect.Type, queryVal any) (func(reflect.Value) (bool, error), bool, error) {
	if !isOrderedOp(op) {
		return nil, false, nil
	}

	qNum, ok := numericValueFromRaw(queryVal)
	if !ok {
		return nil, false, nil
	}

	isPtrField := fieldType.Kind() == reflect.Pointer
	cmpType := fieldType
	if isPtrField {
		cmpType = fieldType.Elem()
	}
	if !needsNumericFloatIntBridge(cmpType.Kind(), qNum.kind) {
		return nil, false, nil
	}

	return func(root reflect.Value) (bool, error) {
		fv, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}
		for fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}
		if isPtrField {
			if fv.Kind() != reflect.Pointer || fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}
		lv, ok := numericValueFromReflect(fv)
		if !ok {
			return false, nil
		}
		cmp, ordered := compareNumericValues(lv, qNum)
		if !ordered {
			return false, nil
		}
		return matchOrderedCmp(op, cmp), nil
	}, true, nil
}

func compileNumericCrossCmpFast(op qx.Op, off uintptr, leafType reflect.Type, queryVal any) (func(unsafe.Pointer) (bool, error), bool, error) {
	if !isOrderedOp(op) {
		return nil, false, nil
	}

	qNum, ok := numericValueFromRaw(queryVal)
	if !ok {
		return nil, false, nil
	}

	isPtrField := leafType.Kind() == reflect.Pointer
	cmpType := leafType
	if isPtrField {
		cmpType = leafType.Elem()
	}
	if !needsNumericFloatIntBridge(cmpType.Kind(), qNum.kind) {
		return nil, false, nil
	}

	switch cmpType.Kind() {
	case reflect.Int:
		return makeFastNumericCrossInt[int](op, off, qNum, isPtrField), true, nil
	case reflect.Int8:
		return makeFastNumericCrossInt[int8](op, off, qNum, isPtrField), true, nil
	case reflect.Int16:
		return makeFastNumericCrossInt[int16](op, off, qNum, isPtrField), true, nil
	case reflect.Int32:
		return makeFastNumericCrossInt[int32](op, off, qNum, isPtrField), true, nil
	case reflect.Int64:
		return makeFastNumericCrossInt[int64](op, off, qNum, isPtrField), true, nil
	case reflect.Uint:
		return makeFastNumericCrossUint[uint](op, off, qNum, isPtrField), true, nil
	case reflect.Uint8:
		return makeFastNumericCrossUint[uint8](op, off, qNum, isPtrField), true, nil
	case reflect.Uint16:
		return makeFastNumericCrossUint[uint16](op, off, qNum, isPtrField), true, nil
	case reflect.Uint32:
		return makeFastNumericCrossUint[uint32](op, off, qNum, isPtrField), true, nil
	case reflect.Uint64:
		return makeFastNumericCrossUint[uint64](op, off, qNum, isPtrField), true, nil
	case reflect.Uintptr:
		return makeFastNumericCrossUint[uintptr](op, off, qNum, isPtrField), true, nil
	case reflect.Float32:
		return makeFastNumericCrossFloat[float32](op, off, qNum, isPtrField), true, nil
	case reflect.Float64:
		return makeFastNumericCrossFloat[float64](op, off, qNum, isPtrField), true, nil
	default:
		return nil, false, nil
	}
}

func makeFastNumericCrossInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](op qx.Op, off uintptr, qNum numericValue, isPtr bool) func(unsafe.Pointer) (bool, error) {
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := numericValue{kind: numericKindInt, i: int64(*(*T)(unsafe.Add(ptr, off)))}
			cmp, ordered := compareNumericValues(lv, qNum)
			if !ordered {
				return false, nil
			}
			return matchOrderedCmp(op, cmp), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		lv := numericValue{kind: numericKindInt, i: int64(*p)}
		cmp, ordered := compareNumericValues(lv, qNum)
		if !ordered {
			return false, nil
		}
		return matchOrderedCmp(op, cmp), nil
	}
}

func makeFastNumericCrossUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr](op qx.Op, off uintptr, qNum numericValue, isPtr bool) func(unsafe.Pointer) (bool, error) {
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := numericValue{kind: numericKindUint, u: uint64(*(*T)(unsafe.Add(ptr, off)))}
			cmp, ordered := compareNumericValues(lv, qNum)
			if !ordered {
				return false, nil
			}
			return matchOrderedCmp(op, cmp), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		lv := numericValue{kind: numericKindUint, u: uint64(*p)}
		cmp, ordered := compareNumericValues(lv, qNum)
		if !ordered {
			return false, nil
		}
		return matchOrderedCmp(op, cmp), nil
	}
}

func makeFastNumericCrossFloat[T ~float32 | ~float64](op qx.Op, off uintptr, qNum numericValue, isPtr bool) func(unsafe.Pointer) (bool, error) {
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			lv := numericValue{kind: numericKindFloat, f: float64(*(*T)(unsafe.Add(ptr, off)))}
			cmp, ordered := compareNumericValues(lv, qNum)
			if !ordered {
				return false, nil
			}
			return matchOrderedCmp(op, cmp), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**T)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		lv := numericValue{kind: numericKindFloat, f: float64(*p)}
		cmp, ordered := compareNumericValues(lv, qNum)
		if !ordered {
			return false, nil
		}
		return matchOrderedCmp(op, cmp), nil
	}
}

func buildDiffFastEq(off uintptr, t reflect.Type) diffUnsafeEqFunc {
	switch t.Kind() {
	case reflect.Bool:
		return makeDiffFastScalar[bool](off)
	case reflect.Int:
		return makeDiffFastScalar[int](off)
	case reflect.Int8:
		return makeDiffFastScalar[int8](off)
	case reflect.Int16:
		return makeDiffFastScalar[int16](off)
	case reflect.Int32:
		return makeDiffFastScalar[int32](off)
	case reflect.Int64:
		return makeDiffFastScalar[int64](off)
	case reflect.Uint:
		return makeDiffFastScalar[uint](off)
	case reflect.Uint8:
		return makeDiffFastScalar[uint8](off)
	case reflect.Uint16:
		return makeDiffFastScalar[uint16](off)
	case reflect.Uint32:
		return makeDiffFastScalar[uint32](off)
	case reflect.Uint64:
		return makeDiffFastScalar[uint64](off)
	case reflect.Uintptr:
		return makeDiffFastScalar[uintptr](off)
	case reflect.Float32:
		return makeDiffFastScalar[float32](off)
	case reflect.Float64:
		return makeDiffFastScalar[float64](off)
	case reflect.String:
		return makeDiffFastScalar[string](off)
	case reflect.Pointer:
		switch t.Elem().Kind() {
		case reflect.Bool:
			return makeDiffFastPtrScalar[bool](off)
		case reflect.Int:
			return makeDiffFastPtrScalar[int](off)
		case reflect.Int8:
			return makeDiffFastPtrScalar[int8](off)
		case reflect.Int16:
			return makeDiffFastPtrScalar[int16](off)
		case reflect.Int32:
			return makeDiffFastPtrScalar[int32](off)
		case reflect.Int64:
			return makeDiffFastPtrScalar[int64](off)
		case reflect.Uint:
			return makeDiffFastPtrScalar[uint](off)
		case reflect.Uint8:
			return makeDiffFastPtrScalar[uint8](off)
		case reflect.Uint16:
			return makeDiffFastPtrScalar[uint16](off)
		case reflect.Uint32:
			return makeDiffFastPtrScalar[uint32](off)
		case reflect.Uint64:
			return makeDiffFastPtrScalar[uint64](off)
		case reflect.Uintptr:
			return makeDiffFastPtrScalar[uintptr](off)
		case reflect.Float32:
			return makeDiffFastPtrScalar[float32](off)
		case reflect.Float64:
			return makeDiffFastPtrScalar[float64](off)
		case reflect.String:
			return makeDiffFastPtrScalar[string](off)
		}
	}

	return nil
}

func makeDiffFastScalar[T comparable](off uintptr) diffUnsafeEqFunc {
	return func(p1, p2 unsafe.Pointer) bool {
		return *(*T)(unsafe.Add(p1, off)) == *(*T)(unsafe.Add(p2, off))
	}
}

func makeDiffFastPtrScalar[T comparable](off uintptr) diffUnsafeEqFunc {
	return func(p1, p2 unsafe.Pointer) bool {
		left := *(**T)(unsafe.Add(p1, off))
		right := *(**T)(unsafe.Add(p2, off))
		if left == nil || right == nil {
			return left == right
		}
		return *left == *right
	}
}

func compileSliceOp(op qx.Op, index []int, rootType, fieldType reflect.Type, queryVal any) (matchFunc, error) {

	isSliceField := false

	switch fieldType.Kind() {

	case reflect.Slice:
		isSliceField = true

	case reflect.Pointer:
		if fieldType.Elem().Kind() == reflect.Slice {
			isSliceField = true
		}
	}

	switch op {

	case qx.OpHASALL, qx.OpHASANY:
		if !isSliceField {
			return nil, fmt.Errorf("%v expects a slice (or *slice) field, got %v", op, fieldType)
		}

	case qx.OpIN:
		if isSliceField {
			return nil, fmt.Errorf("IN expects scalar (or *scalar) field, got %v", fieldType)
		}

	default:
		return nil, fmt.Errorf("unsupported op: %v", op)
	}

	if queryVal == nil {
		return nil, fmt.Errorf("value must be a slice for %v, got nil", op)
	}

	qVal := reflect.ValueOf(queryVal)
	for qVal.Kind() == reflect.Interface || qVal.Kind() == reflect.Pointer {
		if qVal.IsNil() {
			return nil, fmt.Errorf("value must be a slice for %v, got nil", op)
		}
		qVal = qVal.Elem()
	}

	if qVal.Kind() == reflect.Slice && qVal.IsNil() {
		qVal = reflect.MakeSlice(qVal.Type(), 0, 0)
	}
	if qVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("value must be a slice for %v, got %v", op, qVal.Kind())
	}

	// HAS([])     - true
	// HASANY([])  - false
	// IN([])      - false

	if qVal.Len() == 0 {
		switch op {
		case qx.OpHASALL:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return true, nil }, nil
		case qx.OpHASANY:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return false, nil }, nil
		case qx.OpIN:
			return func(_ unsafe.Pointer, _ reflect.Value) (bool, error) { return false, nil }, nil
		}
	}

	if op == qx.OpIN {
		return compileStrictIN(index, rootType, fieldType, qVal)
	}

	elemType := fieldType.Elem()
	if fieldType.Kind() == reflect.Pointer {
		elemType = fieldType.Elem().Elem()
	}

	elemChecks := make([]func(reflect.Value) bool, 0, qVal.Len())
	for i := 0; i < qVal.Len(); i++ {
		check, err := compileSliceElemEq(elemType, qVal.Index(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("%v value at index %d: %w", op, i, err)
		}
		elemChecks = append(elemChecks, check)
	}

	if check, ok := compileStringSliceOp(op, index, rootType, fieldType, qVal); ok {
		return check, nil
	}

	evalSliceOp := func(fieldSlice reflect.Value) bool {
		if fieldSlice.IsNil() || fieldSlice.Len() == 0 {
			// empty field slice cannot satisfy non-empty query
			return false
		}

		if op == qx.OpHASALL {
			for i := 0; i < len(elemChecks); i++ {
				sLen := fieldSlice.Len()
				matchItem := elemChecks[i]
				found := false
				for j := 0; j < sLen; j++ {
					if matchItem(fieldSlice.Index(j)) {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		}

		// OpHASANY
		l := qVal.Len()
		if l == 0 {
			return false
		}
		for i := 0; i < l; i++ {
			sLen := fieldSlice.Len()
			matchItem := elemChecks[i]
			for j := 0; j < sLen; j++ {
				if matchItem(fieldSlice.Index(j)) {
					return true
				}
			}
		}

		return false
	}

	// HAS / HASANY slow path
	slow := func(root reflect.Value) (bool, error) {
		fieldSlice, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}

		for fieldSlice.Kind() == reflect.Interface {
			if fieldSlice.IsNil() {
				// missing/nil slice cannot satisfy non-empty query for HAS/HASANY
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()

			if fieldSlice.IsNil() {
				// missing/nil slice cannot satisfy non-empty query for HAS/HASANY
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()
		}
		if fieldSlice.Kind() == reflect.Pointer {
			if fieldSlice.IsNil() {
				// nil *slice treated as empty slice
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()
		}
		if fieldSlice.Kind() != reflect.Slice {
			// should not happen with compile-time checks, but keep safe
			return false, nil
		}
		return evalSliceOp(fieldSlice), nil
	}

	fast, hasFast := compileSliceFast(index, rootType, fieldType, evalSliceOp)
	if !hasFast {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, nil
	}

	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		if ptr != nil {
			return fast(ptr)
		}
		return slow(root)
	}, nil
}

func compileSliceElemEq(elemType reflect.Type, queryVal any) (func(reflect.Value) bool, error) {
	if isNilLikeQueryValue(queryVal) {
		return func(v reflect.Value) bool {
			for v.Kind() == reflect.Interface {
				if v.IsNil() {
					return true
				}
				v = v.Elem()
			}
			return isNilableAndNil(v)
		}, nil
	}

	if elemType.Kind() == reflect.Interface {
		if qNum, ok := numericValueFromRaw(queryVal); ok {
			qv, err := prepareValue(elemType, queryVal)
			if err != nil {
				return nil, err
			}
			return func(v reflect.Value) bool {
				nv := v
				for nv.Kind() == reflect.Interface {
					if nv.IsNil() {
						return false
					}
					nv = nv.Elem()
				}
				for nv.Kind() == reflect.Pointer {
					if nv.IsNil() {
						return false
					}
					nv = nv.Elem()
				}
				if lv, ok := numericValueFromReflect(nv); ok {
					cmp, ordered := compareNumericValues(lv, qNum)
					return ordered && cmp == 0
				}
				return areEqual(v, qv)
			}, nil
		}

		qv, err := prepareValue(elemType, queryVal)
		if err != nil {
			return nil, err
		}
		return func(v reflect.Value) bool {
			return areEqual(v, qv)
		}, nil
	}

	isPtrElem := elemType.Kind() == reflect.Pointer
	cmpType := elemType
	if isPtrElem {
		cmpType = elemType.Elem()
	}

	if numeric, ok, err := compileSliceElemNumericCrossEq(elemType, queryVal); ok || err != nil {
		return numeric, err
	}

	qv, err := prepareValue(cmpType, queryVal)
	if err != nil {
		return nil, err
	}

	getVal := func(v reflect.Value) (reflect.Value, bool) {
		for v.Kind() == reflect.Interface {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		if isPtrElem {
			if v.Kind() != reflect.Pointer || v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		return v, true
	}

	switch cmpType.Kind() {
	case reflect.Bool:
		q := qv.Bool()
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && val.Bool() == q
		}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		q := qv.Int()
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && val.Int() == q
		}, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		q := qv.Uint()
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && val.Uint() == q
		}, nil

	case reflect.Float32, reflect.Float64:
		q := qv.Float()
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && val.Float() == q
		}, nil

	case reflect.String:
		q := qv.String()
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && val.String() == q
		}, nil
	}

	eqFast, hasFast := getTypeEqFunc(cmpType)
	useEqFast := hasFast && qv.IsValid() && qv.Type() == cmpType
	if useEqFast {
		return func(v reflect.Value) bool {
			val, ok := getVal(v)
			return ok && eqFast(val, qv)
		}, nil
	}

	return func(v reflect.Value) bool {
		val, ok := getVal(v)
		return ok && areEqual(val, qv)
	}, nil
}

func compileSliceElemNumericCrossEq(elemType reflect.Type, queryVal any) (func(reflect.Value) bool, bool, error) {
	qNum, ok := numericValueFromRaw(queryVal)
	if !ok {
		return nil, false, nil
	}

	isPtrElem := elemType.Kind() == reflect.Pointer
	cmpType := elemType
	if isPtrElem {
		cmpType = elemType.Elem()
	}
	if !needsNumericFloatIntBridge(cmpType.Kind(), qNum.kind) {
		return nil, false, nil
	}

	return func(v reflect.Value) bool {
		for v.Kind() == reflect.Interface {
			if v.IsNil() {
				return false
			}
			v = v.Elem()
		}
		if isPtrElem {
			if v.Kind() != reflect.Pointer || v.IsNil() {
				return false
			}
			v = v.Elem()
		}

		lv, ok := numericValueFromReflect(v)
		if !ok {
			return false
		}
		cmp, ordered := compareNumericValues(lv, qNum)
		return ordered && cmp == 0
	}, true, nil
}

func compileStringSliceOp(op qx.Op, index []int, rootType, fieldType reflect.Type, qVal reflect.Value) (matchFunc, bool) {
	isPtrField := false

	switch {
	case fieldType == reflect.TypeOf([]string(nil)):
	case fieldType.Kind() == reflect.Pointer && fieldType.Elem() == reflect.TypeOf([]string(nil)):
		isPtrField = true
	default:
		return nil, false
	}

	qs := make([]string, 0, qVal.Len())
	for i := 0; i < qVal.Len(); i++ {
		raw := qVal.Index(i).Interface()
		if isNilLikeQueryValue(raw) {
			return nil, false
		}

		qv, err := prepareValue(reflect.TypeOf(""), raw)
		if err != nil {
			return nil, false
		}
		qs = append(qs, qv.String())
	}

	qs = dedupStrings(qs)
	if len(qs) == 0 {
		return nil, false
	}

	slow := func(root reflect.Value) (bool, error) {
		fieldSlice, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}
		if isPtrField {
			if fieldSlice.Kind() != reflect.Pointer || fieldSlice.IsNil() {
				return false, nil
			}
			fieldSlice = fieldSlice.Elem()
		}
		if fieldSlice.Kind() != reflect.Slice {
			return false, nil
		}
		return matchStringSliceReflect(op, fieldSlice, qs), nil
	}

	off, leafType, ok := calcFastOffset(rootType, index)
	if !ok || leafType != fieldType {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, true
	}

	fast := makeFastStringSliceOp(op, off, qs, isPtrField)
	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		if ptr != nil {
			return fast(ptr)
		}
		return slow(root)
	}, true
}

func compileStrictIN(index []int, rootType, fieldType reflect.Type, qVal reflect.Value) (matchFunc, error) {
	if qVal.Len() == 1 {
		check, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(0).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 0: %w", err)
		}
		return check, nil
	}

	if check, ok, err := compileStringIN(index, rootType, fieldType, qVal); ok || err != nil {
		return check, err
	}

	switch qVal.Len() {

	case 2:
		left, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(0).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 0: %w", err)
		}
		right, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(1).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 1: %w", err)
		}
		return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
			ok, e := left(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			return right(ptr, root)
		}, nil

	case 3:
		left, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(0).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 0: %w", err)
		}
		mid, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(1).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 1: %w", err)
		}
		right, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(2).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 2: %w", err)
		}
		return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
			ok, e := left(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			ok, e = mid(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			return right(ptr, root)
		}, nil

	case 4:
		a, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(0).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 0: %w", err)
		}
		b, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(1).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 1: %w", err)
		}
		c, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(2).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 2: %w", err)
		}
		d, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(3).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index 3: %w", err)
		}
		return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
			ok, e := a(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			ok, e = b(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			ok, e = c(ptr, root)
			if e != nil || ok {
				return ok, e
			}
			return d(ptr, root)
		}, nil
	}

	checks := make([]matchFunc, 0, qVal.Len())
	for i := 0; i < qVal.Len(); i++ {
		check, err := compileScalarHybrid(qx.OpEQ, index, rootType, fieldType, qVal.Index(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("IN value at index %d: %w", i, err)
		}
		checks = append(checks, check)
	}

	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		for _, check := range checks {
			ok, err := check(ptr, root)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}, nil
}

func compileStringIN(index []int, rootType, fieldType reflect.Type, qVal reflect.Value) (matchFunc, bool, error) {
	isPtrField := fieldType.Kind() == reflect.Pointer

	switch {
	case fieldType.Kind() == reflect.String:
	case isPtrField && fieldType.Elem().Kind() == reflect.String:
	default:
		return nil, false, nil
	}

	qs := make([]string, qVal.Len())
	for i := 0; i < qVal.Len(); i++ {
		raw := qVal.Index(i).Interface()
		if isNilLikeQueryValue(raw) {
			return nil, false, nil
		}

		qv, err := prepareValue(reflect.TypeOf(""), raw)
		if err != nil {
			return nil, true, fmt.Errorf("IN value at index %d: %w", i, err)
		}
		qs[i] = qv.String()
	}

	slow := func(root reflect.Value) (bool, error) {
		fv, ok := getSafeField(root, index)
		if !ok {
			return false, nil
		}
		for fv.Kind() == reflect.Interface {
			if fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}
		if isPtrField {
			if fv.Kind() != reflect.Pointer || fv.IsNil() {
				return false, nil
			}
			fv = fv.Elem()
		}
		if fv.Kind() != reflect.String {
			return false, nil
		}
		return containsString(fv.String(), qs), nil
	}

	off, leafType, ok := calcFastOffset(rootType, index)
	if !ok || leafType != fieldType {
		return func(_ unsafe.Pointer, root reflect.Value) (bool, error) {
			return slow(root)
		}, true, nil
	}

	fast := makeFastStringIN(off, qs, isPtrField)
	return func(ptr unsafe.Pointer, root reflect.Value) (bool, error) {
		if ptr != nil {
			return fast(ptr)
		}
		return slow(root)
	}, true, nil
}

func makeFastStringIN(off uintptr, qs []string, isPtr bool) func(unsafe.Pointer) (bool, error) {
	switch len(qs) {

	case 0:
		return func(_ unsafe.Pointer) (bool, error) { return false, nil }

	case 1:
		q0 := qs[0]
		if !isPtr {
			return func(ptr unsafe.Pointer) (bool, error) {
				return *(*string)(unsafe.Add(ptr, off)) == q0, nil
			}
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			return *p == q0, nil
		}

	case 2:
		q0, q1 := qs[0], qs[1]
		if !isPtr {
			return func(ptr unsafe.Pointer) (bool, error) {
				s := *(*string)(unsafe.Add(ptr, off))
				return s == q0 || s == q1, nil
			}
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			s := *p
			return s == q0 || s == q1, nil
		}

	case 3:
		q0, q1, q2 := qs[0], qs[1], qs[2]
		if !isPtr {
			return func(ptr unsafe.Pointer) (bool, error) {
				s := *(*string)(unsafe.Add(ptr, off))
				return s == q0 || s == q1 || s == q2, nil
			}
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			s := *p
			return s == q0 || s == q1 || s == q2, nil
		}

	case 4:
		q0, q1, q2, q3 := qs[0], qs[1], qs[2], qs[3]
		if !isPtr {
			return func(ptr unsafe.Pointer) (bool, error) {
				s := *(*string)(unsafe.Add(ptr, off))
				return s == q0 || s == q1 || s == q2 || s == q3, nil
			}
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			s := *p
			return s == q0 || s == q1 || s == q2 || s == q3, nil
		}

	default:
		if !isPtr {
			return func(ptr unsafe.Pointer) (bool, error) {
				s := *(*string)(unsafe.Add(ptr, off))
				for i := range qs {
					if s == qs[i] {
						return true, nil
					}
				}
				return false, nil
			}
		}
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(**string)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			s := *p
			for i := range qs {
				if s == qs[i] {
					return true, nil
				}
			}
			return false, nil
		}
	}
}

func containsString(s string, qs []string) bool {
	switch len(qs) {
	case 0:
		return false
	case 1:
		return s == qs[0]
	case 2:
		return s == qs[0] || s == qs[1]
	case 3:
		return s == qs[0] || s == qs[1] || s == qs[2]
	case 4:
		return s == qs[0] || s == qs[1] || s == qs[2] || s == qs[3]
	default:
		for i := range qs {
			if s == qs[i] {
				return true
			}
		}
		return false
	}
}

func isNilLikeQueryValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func isNilableType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return true
	default:
		return false
	}
}

func makeFastStringSliceOp(op qx.Op, off uintptr, qs []string, isPtr bool) func(unsafe.Pointer) (bool, error) {
	if !isPtr {
		return func(ptr unsafe.Pointer) (bool, error) {
			return matchStringSlice(op, *(*[]string)(unsafe.Add(ptr, off)), qs), nil
		}
	}
	return func(ptr unsafe.Pointer) (bool, error) {
		p := *(**[]string)(unsafe.Add(ptr, off))
		if p == nil {
			return false, nil
		}
		return matchStringSlice(op, *p, qs), nil
	}
}

func matchStringSliceReflect(op qx.Op, fieldSlice reflect.Value, qs []string) bool {
	if fieldSlice.IsNil() || fieldSlice.Len() == 0 {
		return false
	}

	switch op {

	case qx.OpHASANY:
		switch len(qs) {

		case 1:
			q0 := qs[0]
			for i := 0; i < fieldSlice.Len(); i++ {
				if fieldSlice.Index(i).String() == q0 {
					return true
				}
			}
			return false

		case 2:
			q0, q1 := qs[0], qs[1]
			for i := 0; i < fieldSlice.Len(); i++ {
				s := fieldSlice.Index(i).String()
				if s == q0 || s == q1 {
					return true
				}
			}
			return false

		case 3:
			q0, q1, q2 := qs[0], qs[1], qs[2]
			for i := 0; i < fieldSlice.Len(); i++ {
				s := fieldSlice.Index(i).String()
				if s == q0 || s == q1 || s == q2 {
					return true
				}
			}
			return false

		case 4:
			q0, q1, q2, q3 := qs[0], qs[1], qs[2], qs[3]
			for i := 0; i < fieldSlice.Len(); i++ {
				s := fieldSlice.Index(i).String()
				if s == q0 || s == q1 || s == q2 || s == q3 {
					return true
				}
			}
			return false

		default:
			for i := 0; i < fieldSlice.Len(); i++ {
				s := fieldSlice.Index(i).String()
				for j := range qs {
					if s == qs[j] {
						return true
					}
				}
			}
			return false
		}

	case qx.OpHASALL:
		return matchStringSliceReflectHAS(fieldSlice, qs)

	default:
		return false
	}
}

func matchStringSliceReflectHAS(fieldSlice reflect.Value, qs []string) bool {
	switch len(qs) {

	case 1:
		q0 := qs[0]
		for i := 0; i < fieldSlice.Len(); i++ {
			if fieldSlice.Index(i).String() == q0 {
				return true
			}
		}
		return false

	case 2:
		q0, q1 := qs[0], qs[1]
		found0, found1 := false, false
		for i := 0; i < fieldSlice.Len(); i++ {
			s := fieldSlice.Index(i).String()
			if s == q0 {
				found0 = true
			}
			if s == q1 {
				found1 = true
			}
			if found0 && found1 {
				return true
			}
		}
		return false

	case 3:
		q0, q1, q2 := qs[0], qs[1], qs[2]
		found0, found1, found2 := false, false, false
		for i := 0; i < fieldSlice.Len(); i++ {
			s := fieldSlice.Index(i).String()
			if s == q0 {
				found0 = true
			}
			if s == q1 {
				found1 = true
			}
			if s == q2 {
				found2 = true
			}
			if found0 && found1 && found2 {
				return true
			}
		}
		return false

	case 4:
		q0, q1, q2, q3 := qs[0], qs[1], qs[2], qs[3]
		found0, found1, found2, found3 := false, false, false, false
		for i := 0; i < fieldSlice.Len(); i++ {
			s := fieldSlice.Index(i).String()
			if s == q0 {
				found0 = true
			}
			if s == q1 {
				found1 = true
			}
			if s == q2 {
				found2 = true
			}
			if s == q3 {
				found3 = true
			}
			if found0 && found1 && found2 && found3 {
				return true
			}
		}
		return false

	default:
		for i := range qs {
			found := false
			q := qs[i]
			for j := 0; j < fieldSlice.Len(); j++ {
				if fieldSlice.Index(j).String() == q {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
}

func matchStringSlice(op qx.Op, fieldSlice []string, qs []string) bool {
	if len(fieldSlice) == 0 {
		return false
	}

	switch op {

	case qx.OpHASANY:
		switch len(qs) {

		case 1:
			q0 := qs[0]
			for _, s := range fieldSlice {
				if s == q0 {
					return true
				}
			}
			return false

		case 2:
			q0, q1 := qs[0], qs[1]
			for _, s := range fieldSlice {
				if s == q0 || s == q1 {
					return true
				}
			}
			return false

		case 3:
			q0, q1, q2 := qs[0], qs[1], qs[2]
			for _, s := range fieldSlice {
				if s == q0 || s == q1 || s == q2 {
					return true
				}
			}
			return false

		case 4:
			q0, q1, q2, q3 := qs[0], qs[1], qs[2], qs[3]
			for _, s := range fieldSlice {
				if s == q0 || s == q1 || s == q2 || s == q3 {
					return true
				}
			}
			return false

		default:
			for _, s := range fieldSlice {
				for i := range qs {
					if s == qs[i] {
						return true
					}
				}
			}
			return false
		}

	case qx.OpHASALL:
		switch len(qs) {

		case 1:
			q0 := qs[0]
			for _, s := range fieldSlice {
				if s == q0 {
					return true
				}
			}
			return false

		case 2:
			q0, q1 := qs[0], qs[1]
			found0, found1 := false, false
			for _, s := range fieldSlice {
				if s == q0 {
					found0 = true
				}
				if s == q1 {
					found1 = true
				}
				if found0 && found1 {
					return true
				}
			}
			return false

		case 3:
			q0, q1, q2 := qs[0], qs[1], qs[2]
			found0, found1, found2 := false, false, false
			for _, s := range fieldSlice {
				if s == q0 {
					found0 = true
				}
				if s == q1 {
					found1 = true
				}
				if s == q2 {
					found2 = true
				}
				if found0 && found1 && found2 {
					return true
				}
			}
			return false

		case 4:
			q0, q1, q2, q3 := qs[0], qs[1], qs[2], qs[3]
			found0, found1, found2, found3 := false, false, false, false
			for _, s := range fieldSlice {
				if s == q0 {
					found0 = true
				}
				if s == q1 {
					found1 = true
				}
				if s == q2 {
					found2 = true
				}
				if s == q3 {
					found3 = true
				}
				if found0 && found1 && found2 && found3 {
					return true
				}
			}
			return false
		default:
			for i := range qs {
				found := false
				q := qs[i]
				for _, s := range fieldSlice {
					if s == q {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		}
	default:
		return false
	}
}

func dedupStrings(v []string) []string {
	if len(v) < 2 {
		return v
	}

	out := v[:0]
	for i := range v {
		s := v[i]
		dup := false
		for j := range out {
			if out[j] == s {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, s)
		}
	}
	return out
}

func compileSliceFast(index []int, rootType, fieldType reflect.Type, evalFn func(fieldSlice reflect.Value) bool) (func(unsafe.Pointer) (bool, error), bool) {
	off, leafType, ok := calcFastOffset(rootType, index)
	if !ok || leafType != fieldType {
		return nil, false
	}

	if fieldType.Kind() == reflect.Slice {
		return func(ptr unsafe.Pointer) (bool, error) {
			fieldSlice := reflect.NewAt(fieldType, unsafe.Add(ptr, off)).Elem()
			return evalFn(fieldSlice), nil
		}, true
	}

	if fieldType.Kind() == reflect.Pointer && fieldType.Elem().Kind() == reflect.Slice {
		sliceType := fieldType.Elem()
		return func(ptr unsafe.Pointer) (bool, error) {
			p := *(*unsafe.Pointer)(unsafe.Add(ptr, off))
			if p == nil {
				return false, nil
			}
			fieldSlice := reflect.NewAt(sliceType, p).Elem()
			return evalFn(fieldSlice), nil
		}, true
	}

	return nil, false
}

func calcFastOffset(t reflect.Type, index []int) (off uintptr, leaf reflect.Type, ok bool) {
	for depth, i := range index {
		if t.Kind() != reflect.Struct {
			return 0, nil, false
		}
		f := t.Field(i)
		off += f.Offset
		t = f.Type
		if depth < len(index)-1 {
			if t.Kind() == reflect.Pointer || t.Kind() == reflect.Interface {
				return 0, nil, false
			}
		}
	}
	return off, t, true
}

func getSafeField(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return reflect.Value{}, false
		}
		v = v.Field(i)
	}
	return v, true
}

func zeroNilableValue(t reflect.Type) (reflect.Value, bool) {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return reflect.Zero(t), true
	default:
		return reflect.Value{}, false
	}
}

func isStrictScalarKind(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return true
	default:
		return false
	}
}

func signedTargetCanHoldUint(bits int, v uint64) bool {
	if bits >= 64 {
		return v <= ^uint64(0)>>1
	}
	return v < uint64(1)<<(bits-1)
}

func coerceValueExact(targetType reflect.Type, rv reflect.Value) (reflect.Value, bool) {
	targetZero := reflect.Zero(targetType)

	switch targetType.Kind() {
	case reflect.Bool:
		if rv.Kind() != reflect.Bool {
			return reflect.Value{}, false
		}
		return rv.Convert(targetType), true

	case reflect.String:
		if rv.Kind() != reflect.String {
			return reflect.Value{}, false
		}
		return rv.Convert(targetType), true

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.CanInt() {
			i := rv.Int()
			if targetZero.OverflowInt(i) {
				return reflect.Value{}, false
			}
			return rv.Convert(targetType), true
		}
		if rv.CanUint() {
			u := rv.Uint()
			if !signedTargetCanHoldUint(targetType.Bits(), u) {
				return reflect.Value{}, false
			}
			return rv.Convert(targetType), true
		}
		return reflect.Value{}, false

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if rv.CanUint() {
			u := rv.Uint()
			if targetZero.OverflowUint(u) {
				return reflect.Value{}, false
			}
			return rv.Convert(targetType), true
		}
		if rv.CanInt() {
			i := rv.Int()
			if i < 0 || targetZero.OverflowUint(uint64(i)) {
				return reflect.Value{}, false
			}
			return rv.Convert(targetType), true
		}
		return reflect.Value{}, false

	case reflect.Float32, reflect.Float64:
		if !rv.CanFloat() {
			return reflect.Value{}, false
		}
		f := rv.Float()
		if targetZero.OverflowFloat(f) {
			return reflect.Value{}, false
		}
		converted := rv.Convert(targetType)
		roundTrip := converted.Convert(rv.Type()).Float()
		if math.IsNaN(f) && math.IsNaN(roundTrip) {
			return converted, true
		}
		return converted, roundTrip == f

	case reflect.Complex64, reflect.Complex128:
		if !rv.CanComplex() {
			return reflect.Value{}, false
		}
		converted := rv.Convert(targetType)
		return converted, converted.Convert(rv.Type()).Complex() == rv.Complex()
	}

	return reflect.Value{}, false
}

func prepareValue(targetType reflect.Type, raw any) (reflect.Value, error) {

	if raw == nil {
		if zero, ok := zeroNilableValue(targetType); ok {
			return zero, nil
		}
		return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
	}

	rv := reflect.ValueOf(raw)

	for rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			if zero, ok := zeroNilableValue(targetType); ok {
				return zero, nil
			}
			return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
		}
		rv = rv.Elem()
	}

	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			if zero, ok := zeroNilableValue(targetType); ok {
				return zero, nil
			}
			return reflect.Value{}, fmt.Errorf("cannot use nil for type %v", targetType)
		}
		rv = rv.Elem()
	}

	if targetType.Kind() == reflect.Interface {
		return rv, nil
	}
	if rv.Type() == targetType {
		return rv, nil
	}
	if converted, ok := coerceValueExact(targetType, rv); ok {
		return converted, nil
	}
	if !isStrictScalarKind(targetType.Kind()) && rv.Type().ConvertibleTo(targetType) {
		return rv.Convert(targetType), nil
	}
	return reflect.Value{}, fmt.Errorf("type mismatch: field is %v, value is %v", targetType, rv.Type())
}

type numericValueKind uint8

const (
	numericKindInvalid numericValueKind = iota
	numericKindInt
	numericKindUint
	numericKindFloat
)

type numericValue struct {
	kind numericValueKind
	i    int64
	u    uint64
	f    float64
}

func numericValueFromRaw(raw any) (numericValue, bool) {
	if raw == nil {
		return numericValue{}, false
	}
	rv := reflect.ValueOf(raw)
	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return numericValue{}, false
		}
		rv = rv.Elem()
	}
	return numericValueFromReflect(rv)
}

func numericValueFromReflect(rv reflect.Value) (numericValue, bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return numericValue{kind: numericKindInt, i: rv.Int()}, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return numericValue{kind: numericKindUint, u: rv.Uint()}, true
	case reflect.Float32, reflect.Float64:
		return numericValue{kind: numericKindFloat, f: rv.Float()}, true
	default:
		return numericValue{}, false
	}
}

func needsNumericFloatIntBridge(fieldKind reflect.Kind, queryKind numericValueKind) bool {
	switch {
	case isSignedIntKind(fieldKind), isUnsignedIntKind(fieldKind):
		return queryKind == numericKindFloat
	case isFloatKind(fieldKind):
		return queryKind == numericKindInt || queryKind == numericKindUint
	default:
		return false
	}
}

func isSignedIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func isUnsignedIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	default:
		return false
	}
}

func isFloatKind(k reflect.Kind) bool {
	switch k {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func compareNumericValues(left, right numericValue) (int, bool) {
	switch left.kind {
	case numericKindInt:
		switch right.kind {
		case numericKindInt:
			return compareInt64(left.i, right.i), true
		case numericKindUint:
			return compareInt64Uint64(left.i, right.u), true
		case numericKindFloat:
			cmp, ok := compareFloat64Int64(right.f, left.i)
			return -cmp, ok
		}
	case numericKindUint:
		switch right.kind {
		case numericKindInt:
			return -compareInt64Uint64(right.i, left.u), true
		case numericKindUint:
			return compareUint64(left.u, right.u), true
		case numericKindFloat:
			cmp, ok := compareFloat64Uint64(right.f, left.u)
			return -cmp, ok
		}
	case numericKindFloat:
		switch right.kind {
		case numericKindInt:
			return compareFloat64Int64(left.f, right.i)
		case numericKindUint:
			return compareFloat64Uint64(left.f, right.u)
		case numericKindFloat:
			if math.IsNaN(left.f) || math.IsNaN(right.f) {
				return 0, false
			}
			return compareFloat64(left.f, right.f), true
		}
	}
	return 0, false
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareUint64(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloat64(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64Uint64(i int64, u uint64) int {
	if i < 0 {
		return -1
	}
	return compareUint64(uint64(i), u)
}

func compareFloat64Int64(f float64, i int64) (int, bool) {
	if math.IsNaN(f) {
		return 0, false
	}
	if math.IsInf(f, 1) {
		return 1, true
	}
	if math.IsInf(f, -1) {
		return -1, true
	}

	if i >= 0 {
		if f < 0 {
			return -1, true
		}
		return comparePositiveFloat64Uint64(f, uint64(i)), true
	}
	if f >= 0 {
		return 1, true
	}

	magnitude := uint64(-(i + 1))
	magnitude++

	cmp := comparePositiveFloat64Uint64(-f, magnitude)
	return -cmp, true
}

func compareFloat64Uint64(f float64, u uint64) (int, bool) {
	if math.IsNaN(f) {
		return 0, false
	}
	if math.IsInf(f, 1) {
		return 1, true
	}
	if math.IsInf(f, -1) {
		return -1, true
	}
	if f < 0 {
		return -1, true
	}
	return comparePositiveFloat64Uint64(f, u), true
}

func comparePositiveFloat64Uint64(f float64, u uint64) int {
	if f == 0 {
		return compareUint64(0, u)
	}

	sig, exp2 := decomposePositiveFloat64(f)
	if exp2 >= 0 {
		if exp2 > 63 || sig > ^uint64(0)>>exp2 {
			return 1
		}
		return compareUint64(sig<<exp2, u)
	}

	shift := -exp2
	if shift >= 64 {
		if u == 0 {
			return 1
		}
		return -1
	}

	intPart := sig >> shift
	if intPart != u {
		return compareUint64(intPart, u)
	}

	mask := (uint64(1) << shift) - 1
	if sig&mask != 0 {
		return 1
	}
	return 0
}

func decomposePositiveFloat64(f float64) (uint64, int) {
	bits := math.Float64bits(f)
	expBits := int((bits >> 52) & 0x7ff)
	frac := bits & ((uint64(1) << 52) - 1)
	if expBits == 0 {
		return frac, -1074
	}
	return (uint64(1) << 52) | frac, expBits - 1075
}

var typeEqCache sync.Map

type typeEqCacheRec struct {
	fn typeEqFunc
	ok bool
}

type typeEqBuildState struct {
	recs  map[reflect.Type]typeEqCacheRec
	stack map[reflect.Type]bool
}

func getTypeEqFunc(t reflect.Type) (typeEqFunc, bool) {
	if v, ok := typeEqCache.Load(t); ok {
		rec := v.(typeEqCacheRec)
		return rec.fn, rec.ok
	}
	fn, ok := buildTypeEqFunc(t)
	typeEqCache.Store(t, typeEqCacheRec{fn: fn, ok: ok})
	return fn, ok
}

func buildTypeEqFunc(t reflect.Type) (typeEqFunc, bool) {
	state := typeEqBuildState{
		recs:  make(map[reflect.Type]typeEqCacheRec),
		stack: make(map[reflect.Type]bool),
	}
	rec := state.build(t)
	return rec.fn, rec.ok
}

func (s *typeEqBuildState) build(t reflect.Type) typeEqCacheRec {
	if rec, ok := s.recs[t]; ok {
		return rec
	}
	if s.stack[t] {
		// Recursive/self-referential type graphs fall back to reflect.DeepEqual.
		return typeEqCacheRec{}
	}

	s.stack[t] = true
	rec := s.buildSlow(t)
	delete(s.stack, t)

	s.recs[t] = rec
	return rec
}

func (s *typeEqBuildState) buildSlow(t reflect.Type) typeEqCacheRec {
	switch t.Kind() {
	case reflect.Bool:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.Bool() == v2.Bool() }, ok: true}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.Int() == v2.Int() }, ok: true}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.Uint() == v2.Uint() }, ok: true}

	case reflect.Float32, reflect.Float64:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.Float() == v2.Float() }, ok: true}

	case reflect.Complex64, reflect.Complex128:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.Complex() == v2.Complex() }, ok: true}

	case reflect.String:
		return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool { return v1.String() == v2.String() }, ok: true}

	case reflect.Pointer:
		elemKind := t.Elem().Kind()
		switch elemKind {
		case reflect.Bool:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Pointer() == v2.Pointer() {
					return true
				}
				return v1.Elem().Bool() == v2.Elem().Bool()
			}, ok: true}

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Pointer() == v2.Pointer() {
					return true
				}
				return v1.Elem().Int() == v2.Elem().Int()
			}, ok: true}

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Pointer() == v2.Pointer() {
					return true
				}
				return v1.Elem().Uint() == v2.Elem().Uint()
			}, ok: true}

		case reflect.Float32, reflect.Float64:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				return v1.Elem().Float() == v2.Elem().Float()
			}, ok: true}

		case reflect.Complex64, reflect.Complex128:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				return v1.Elem().Complex() == v2.Elem().Complex()
			}, ok: true}

		case reflect.String:
			return typeEqCacheRec{fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Pointer() == v2.Pointer() {
					return true
				}
				return v1.Elem().String() == v2.Elem().String()
			}, ok: true}
		}

		elemRec := s.build(t.Elem())
		if !elemRec.ok {
			return typeEqCacheRec{}
		}
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				return elemRec.fn(v1.Elem(), v2.Elem())
			},
			ok: true,
		}

	case reflect.Array:
		elemRec := s.build(t.Elem())
		if !elemRec.ok {
			return typeEqCacheRec{}
		}
		n := t.Len()
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				for i := 0; i < n; i++ {
					if !elemRec.fn(v1.Index(i), v2.Index(i)) {
						return false
					}
				}
				return true
			},
			ok: true,
		}

	case reflect.Slice:
		elemRec := s.build(t.Elem())
		if !elemRec.ok {
			return typeEqCacheRec{}
		}
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Len() != v2.Len() {
					return false
				}
				for i := 0; i < v1.Len(); i++ {
					if !elemRec.fn(v1.Index(i), v2.Index(i)) {
						return false
					}
				}
				return true
			},
			ok: true,
		}

	case reflect.Map:
		elemRec := s.build(t.Elem())
		if !elemRec.ok {
			return typeEqCacheRec{}
		}
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				if v1.Len() != v2.Len() {
					return false
				}
				iter := v1.MapRange()
				for iter.Next() {
					right := v2.MapIndex(iter.Key())
					if !right.IsValid() {
						return false
					}
					if !elemRec.fn(iter.Value(), right) {
						return false
					}
				}
				return true
			},
			ok: true,
		}

	case reflect.Interface:
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				return areEqual(v1.Elem(), v2.Elem())
			},
			ok: true,
		}

	case reflect.Struct:
		n := t.NumField()
		fns := make([]typeEqFunc, n)
		for i := 0; i < n; i++ {
			rec := s.build(t.Field(i).Type)
			if !rec.ok {
				return typeEqCacheRec{}
			}
			fns[i] = rec.fn
		}
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				for i := 0; i < n; i++ {
					if !fns[i](v1.Field(i), v2.Field(i)) {
						return false
					}
				}
				return true
			},
			ok: true,
		}

	case reflect.Chan:
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				if v1.IsNil() || v2.IsNil() {
					return v1.IsNil() == v2.IsNil()
				}
				return v1.Pointer() == v2.Pointer()
			},
			ok: true,
		}

	case reflect.Func:
		return typeEqCacheRec{
			fn: func(v1, v2 reflect.Value) bool {
				return v1.IsNil() && v2.IsNil()
			},
			ok: true,
		}
	}

	return typeEqCacheRec{}
}

func areEqual(v1, v2 reflect.Value) bool {

	// maybe combine interface/pointer checks into single if?

	for v1.Kind() == reflect.Interface {
		if v1.IsNil() {
			return isNilableAndNil(v2)
		}
		v1 = v1.Elem()
	}
	for v2.Kind() == reflect.Interface {
		if v2.IsNil() {
			return isNilableAndNil(v1)
		}
		v2 = v2.Elem()
	}

	if v1.Kind() == reflect.Pointer {
		if v1.IsNil() {
			return v2.Kind() == reflect.Pointer && v2.IsNil()
		}
		v1 = v1.Elem()
	}
	if v2.Kind() == reflect.Pointer {
		if v2.IsNil() {
			return false
		}
		v2 = v2.Elem()
	}

	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}

	// same type
	if v1.Type() == v2.Type() {
		if eqFast, hasFast := getTypeEqFunc(v1.Type()); hasFast {
			return eqFast(v1, v2)
		}
		switch v1.Kind() {
		case reflect.Struct, reflect.Array:
			return reflect.DeepEqual(v1.Interface(), v2.Interface())
		}

		if v1.Comparable() {
			return v1.Equal(v2)
		}
		return reflect.DeepEqual(v1.Interface(), v2.Interface())
	}

	// numeric cross-type fast paths
	if v1.CanInt() && v2.CanInt() {
		return v1.Int() == v2.Int()
	}
	if v1.CanUint() && v2.CanUint() {
		return v1.Uint() == v2.Uint()
	}
	if v1.CanFloat() && v2.CanFloat() {
		return v1.Float() == v2.Float()
	}
	if v1.CanComplex() && v2.CanComplex() {
		return v1.Complex() == v2.Complex()
	}
	if v1.Kind() == reflect.Bool && v2.Kind() == reflect.Bool {
		return v1.Bool() == v2.Bool()
	}
	if v1.Kind() == reflect.String && v2.Kind() == reflect.String {
		return v1.String() == v2.String()
	}

	return false
}

func isNilableAndNil(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}

func isOrderedOp(op qx.Op) bool {
	switch op {
	case qx.OpEQ, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return true
	default:
		return false
	}
}

func matchOrderedCmp(op qx.Op, cmp int) bool {
	switch op {
	case qx.OpEQ:
		return cmp == 0
	case qx.OpGT:
		return cmp > 0
	case qx.OpGTE:
		return cmp >= 0
	case qx.OpLT:
		return cmp < 0
	case qx.OpLTE:
		return cmp <= 0
	default:
		return false
	}
}

func cmpOrderedInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](op qx.Op, lv, q T) bool {
	switch op {
	case qx.OpEQ:
		return lv == q
	case qx.OpGT:
		return lv > q
	case qx.OpGTE:
		return lv >= q
	case qx.OpLT:
		return lv < q
	case qx.OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](op qx.Op, lv, q T) bool {
	switch op {
	case qx.OpEQ:
		return lv == q
	case qx.OpGT:
		return lv > q
	case qx.OpGTE:
		return lv >= q
	case qx.OpLT:
		return lv < q
	case qx.OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedFloat[T ~float32 | ~float64](op qx.Op, lv, q T) bool {
	switch op {
	case qx.OpEQ:
		return lv == q
	case qx.OpGT:
		return lv > q
	case qx.OpGTE:
		return lv >= q
	case qx.OpLT:
		return lv < q
	case qx.OpLTE:
		return lv <= q
	default:
		return false
	}
}

func cmpOrderedString(op qx.Op, lv, q string) bool {
	switch op {
	case qx.OpEQ:
		return lv == q
	case qx.OpGT:
		return lv > q
	case qx.OpGTE:
		return lv >= q
	case qx.OpLT:
		return lv < q
	case qx.OpLTE:
		return lv <= q
	default:
		return false
	}
}
