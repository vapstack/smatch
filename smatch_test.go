package smatch

import (
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	. "github.com/vapstack/qx"
)

type User struct {
	ID       int     `json:"id" db:"pk"`
	Name     string  `json:"name"`
	Age      int     `json:"age"`
	Score    float64 `db:"score_val"`
	IsActive bool

	Tags    []string
	RolePtr *string
	Meta    any

	private string // must not be accessible
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func rawBinary(op Op, field string, value any) Expr {
	return OP(op, REF(field), LIT(value))
}

func TestMatch_ScalarsAndLogic(t *testing.T) {
	u := User{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"admin", "editor"},
		RolePtr:  strPtr("superuser"),
		Meta:     100,
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"EQ int", EQ("Age", 30), true},
		{"EQ int mismatch", EQ("Age", 31), false},
		{"GT int", GT("Age", 20), true},
		{"GTE int", GTE("Age", 30), true},
		{"LT float via db tag", LT("score_val", 100.0), true},
		{"EQ string via json tag", EQ("name", "Alice"), true},
		{"EQ bool", EQ("IsActive", true), true},

		{"AND success", AND(EQ("Age", 30), EQ("Name", "Alice")), true},
		{"AND fail", AND(EQ("Age", 30), EQ("Name", "Bob")), false},
		{"OR success", OR(EQ("Age", 100), EQ("Name", "Alice")), true},
		{"NOT success", NOT(EQ("Name", "Bob")), true},

		// pointers
		{"EQ pointer field with scalar", EQ("RolePtr", "superuser"), true},
		{"EQ pointer mismatch", EQ("RolePtr", "user"), false},

		// numeric cross-type (int vs int64 should match by value)
		{"EQ int field with int64", EQ("Age", int64(30)), true},
		{"EQ float strict", EQ("Score", 95.5), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_NotOnLogicalGroups(t *testing.T) {
	type S struct {
		A int
		B int
	}

	v := S{A: 1, B: 2}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"NOT over AND (true->false)", NOT(AND(EQ("A", 1), EQ("B", 2))), false},
		{"NOT over AND (false->true)", NOT(AND(EQ("A", 1), EQ("B", 9))), true},
		{"NOT over OR (true->false)", NOT(OR(EQ("A", 1), EQ("B", 9))), false},
		{"NOT over OR (false->true)", NOT(OR(EQ("A", 7), EQ("B", 9))), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_DataEquality(t *testing.T) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		Profile Profile
	}

	v := &S{
		Profile: Profile{Level: 5, Rank: intPtr(99)},
	}

	query := Profile{Level: 5, Rank: intPtr(99)}

	ok, err := Match(v, EQ("Profile", query))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected data-equality match for struct with pointer fields")
	}
}

func TestMatch_DataEquality_PointerNaNSemantics(t *testing.T) {
	type Profile struct {
		Rank *float64
	}
	type S struct {
		Profile Profile
	}

	n1 := math.NaN()
	p1 := &n1

	v := S{
		Profile: Profile{Rank: p1},
	}

	// Same pointer still dereferences to NaN, so data-equality must report mismatch.
	ok, err := Match(v, EQ("Profile", Profile{Rank: p1}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected mismatch for same pointer to NaN")
	}

	// Different pointers with NaN payload must not be equal.
	n2 := math.NaN()
	p2 := &n2

	ok, err = Match(v, EQ("Profile", Profile{Rank: p2}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected mismatch for different pointers to NaN")
	}
}

func TestMatch_DataEquality_AcyclicComposite(t *testing.T) {
	type Child struct {
		Rank int
	}
	type Payload struct {
		Labels []string
		Scores map[string]*int
		Meta   any
		Child  *Child
	}
	type S struct {
		Payload Payload
	}

	v := S{
		Payload: Payload{
			Labels: []string{"A", "B"},
			Scores: map[string]*int{
				"one": intPtr(1),
				"two": intPtr(2),
			},
			Meta:  []int{7, 8, 9},
			Child: &Child{Rank: 5},
		},
	}

	query := Payload{
		Labels: []string{"A", "B"},
		Scores: map[string]*int{
			"one": intPtr(1),
			"two": intPtr(2),
		},
		Meta:  []int{7, 8, 9},
		Child: &Child{Rank: 5},
	}

	ok, err := Match(v, EQ("Payload", query))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected data-equality match for acyclic composite payload")
	}
}

func TestMatch_ChannelIdentitySemantics(t *testing.T) {
	type S struct {
		Ch chan int
	}

	shared := make(chan int)
	other := make(chan int)

	ok, err := Match(S{Ch: shared}, EQ("Ch", shared))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected same channel identity to match")
	}

	ok, err = Match(S{Ch: shared}, EQ("Ch", other))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected different channels not to match")
	}
}

func TestMatch_FuncNilOnlySemantics(t *testing.T) {
	type S struct {
		Fn func()
	}

	fn := func() {}

	ok, err := Match(S{}, EQ("Fn", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected nil funcs to match nil query")
	}

	ok, err = Match(S{Fn: fn}, EQ("Fn", fn))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected non-nil funcs not to match, even when identical")
	}
}

func TestMatch_DataEquality_RecursiveFallback(t *testing.T) {
	type Node struct {
		Value int
		Next  *Node
	}
	type S struct {
		Root Node
	}

	n1 := &Node{Value: 1}
	n1.Next = n1

	n2 := &Node{Value: 1}
	n2.Next = n2

	want := reflect.DeepEqual(*n1, *n2)

	ok, err := Match(S{Root: *n1}, EQ("Root", *n2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok != want {
		t.Fatalf("recursive fallback mismatch: got=%v want=%v", ok, want)
	}
}

func TestMatch_SliceOpsSemantics(t *testing.T) {
	u := User{
		Name: "Alice",
		Tags: []string{"A", "B", "C"},
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		// IN: scalar value in query set
		{"IN success", IN("name", []string{"Alice", "Bob"}), true},
		{"IN fail", IN("name", []string{"Bob", "Charlie"}), false},
		{"IN empty query => false", IN("name", []string{}), false},

		// HAS: contains all query items (subset check)
		{"HAS subset", HASALL("Tags", []string{"A", "C"}), true},
		{"HAS fail missing", HASALL("Tags", []string{"A", "Z"}), false},
		{"HAS empty query => true", HASALL("Tags", []string{}), true},

		// HASANY: intersection exists
		{"HASANY common", HASANY("Tags", []string{"Z", "B"}), true},
		{"HASANY fail", HASANY("Tags", []string{"X", "Y"}), false},
		{"HASANY empty query => false", HASANY("Tags", []string{}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_INSingletonMatchesEQ(t *testing.T) {
	role := "admin"
	var nilRole *string

	type S struct {
		ID      int
		RolePtr *string
	}

	cases := []struct {
		name string
		v    S
		in   Expr
		eq   Expr
	}{
		{
			name: "scalar singleton",
			v:    S{ID: 42},
			in:   IN("ID", []int64{42}),
			eq:   EQ("ID", int64(42)),
		},
		{
			name: "pointer singleton nil",
			v:    S{RolePtr: nil},
			in:   IN("RolePtr", []any{nil}),
			eq:   EQ("RolePtr", nil),
		},
		{
			name: "pointer singleton typed nil",
			v:    S{RolePtr: nil},
			in:   IN("RolePtr", []*string{nil}),
			eq:   EQ("RolePtr", nilRole),
		},
		{
			name: "pointer singleton value",
			v:    S{RolePtr: &role},
			in:   IN("RolePtr", []string{"admin"}),
			eq:   EQ("RolePtr", "admin"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotIN, err := Match(tc.v, tc.in)
			if err != nil {
				t.Fatalf("IN unexpected error: %v", err)
			}
			gotEQ, err := Match(tc.v, tc.eq)
			if err != nil {
				t.Fatalf("EQ unexpected error: %v", err)
			}
			if gotIN != gotEQ {
				t.Fatalf("IN=%v EQ=%v", gotIN, gotEQ)
			}
		})
	}
}

func TestMatch_INTypeMismatchErrors(t *testing.T) {
	u := User{ID: 555}

	cases := []struct {
		name string
		expr Expr
	}{
		{"singleton incompatible type must fail", IN("ID", []any{"not-an-int"})},
		{"mixed list with incompatible type must fail", IN("ID", []any{555, "bad"})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Match(u, tc.expr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestMatch_ScalarNilLiteralSemantics(t *testing.T) {
	type inner struct {
		Age int
	}
	type outer struct {
		*inner
	}

	u := User{Age: 42}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"EQ nil on scalar field => false", EQ("Age", nil), false},
		{"EQ typed nil on scalar field => false", EQ("Age", (*int)(nil)), false},
		{"singleton IN typed nil on scalar field => false", IN("Age", []any{(*int)(nil)}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}

	got, err := Match(outer{}, EQ("Age", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatalf("expected nil embedded pointer not to make scalar Age == nil")
	}
}

func TestMatch_NilLiteralOnUnreachableNilablePromotedField(t *testing.T) {
	type inner struct {
		Role *string
	}
	type outer struct {
		*inner
	}

	got, err := Match(outer{}, EQ("Role", nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatalf("expected unreachable nilable promoted field to match nil")
	}
}

func TestMatch_INStringPointerWithNilFallback(t *testing.T) {
	type S struct {
		RolePtr *string
	}

	admin := "admin"

	cases := []struct {
		name string
		v    S
		ok   bool
	}{
		{"nil pointer matches nil arm", S{}, true},
		{"value pointer matches string arm", S{RolePtr: &admin}, true},
		{"other value does not match", S{RolePtr: strPtr("user")}, false},
	}

	expr := IN("RolePtr", []any{nil, "admin"})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.v, expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_SliceOpsWithNilField(t *testing.T) {
	// nil slice field should behave like empty
	u := User{
		Name: "Alice",
		Tags: nil,
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"HAS non-empty on nil field => false", HASALL("Tags", []string{"A"}), false},
		{"HASANY non-empty on nil field => false", HASANY("Tags", []string{"A"}), false},
		{"HAS empty on nil field => true", HASALL("Tags", []string{}), true},
		{"HASANY empty on nil field => false", HASANY("Tags", []string{}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_InterfaceUnwrapAndTypedNil(t *testing.T) {
	u1 := User{Meta: intPtr(123)} // interface holds *int
	u2 := User{Meta: 123}         // interface holds int

	var p *int = nil
	uTypedNil := User{Meta: p} // interface is non-nil, but underlying pointer is nil

	cases := []struct {
		name string
		u    User
		expr Expr
		ok   bool
	}{
		{"interface ptr should match scalar", u1, EQ("Meta", 123), true},
		{"interface scalar should match scalar", u2, EQ("Meta", 123), true},
		{"interface ptr should not match other value", u1, EQ("Meta", 124), false},

		// typed nil inside interface counts as nil for EQ(field, nil)
		{"typed nil inside interface should match nil", uTypedNil, EQ("Meta", nil), true},
		{"typed nil inside interface should not match non-nil", uTypedNil, EQ("Meta", 0), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_InterfaceEquality_StrictTypes(t *testing.T) {
	type MyString string
	type S struct {
		Meta any
	}

	cases := []struct {
		name string
		v    S
		expr Expr
		ok   bool
	}{
		{"string should not equal int via conversion", S{Meta: "A"}, EQ("Meta", 65), false},
		{"named string should still equal plain string", S{Meta: MyString("A")}, EQ("Meta", "A"), true},
		{"numeric int should equal float by value", S{Meta: 10}, EQ("Meta", 10.0), true},
		{"numeric float should not equal int when fractional", S{Meta: 10.5}, EQ("Meta", 10), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_InterfaceOrderedComparisons(t *testing.T) {
	type S struct {
		Meta any
	}

	cases := []struct {
		name string
		v    S
		expr Expr
		ok   bool
	}{
		{"int GT", S{Meta: 123}, GT("Meta", 100), true},
		{"int GTE false", S{Meta: 123}, GTE("Meta", 124), false},
		{"int GT fractional float", S{Meta: 2}, GT("Meta", 1.9), true},
		{"string LT", S{Meta: "beta"}, LT("Meta", "delta"), true},
		{"pointer to int in interface GT", S{Meta: intPtr(123)}, GT("Meta", 100), true},
		{"large float LT adjacent int", S{Meta: float64(1 << 53)}, LT("Meta", int64((1<<53)+1)), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(tc.v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_EqNilOnPointerFields(t *testing.T) {
	u := User{RolePtr: nil}
	var nilRole *string

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"EQ nil on nil pointer => true", EQ("RolePtr", nil), true},
		{"EQ typed nil on nil pointer => true", EQ("RolePtr", nilRole), true},
		{"NOT(EQ nil) on nil pointer => false", NOT(EQ("RolePtr", nil)), false},
		{"EQ scalar on nil pointer => false", EQ("RolePtr", "admin"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(u, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_Errors_TypeAndAccess(t *testing.T) {
	u := User{
		ID:   555,
		Name: "Alice",
		Tags: []string{"A"},
	}

	cases := []struct {
		name string
		expr Expr
	}{
		// private field should not be accessible
		{"private field access must fail", EQ("private", "secret")},

		// scalar comparisons with incompatible types must fail
		{"compare int with string must fail", EQ("ID", "not-an-int")},

		// unsupported op/type combo must fail
		{"GT on slice must fail", GT("Tags", 5)},
		{"PREFIX on int must fail", PREFIX("Age", 1)},
		{"SUFFIX on int must fail", SUFFIX("Age", 1)},
		{"CONTAINS on int must fail", CONTAINS("Age", 1)},

		// IN/HAS/HASANY require slice query values
		{"IN with scalar RHS must fail", rawBinary(OpIN, "ID", 123)},
		{"HAS with scalar RHS must fail", rawBinary(OpHASALL, "Tags", 123)},
		{"HASANY with scalar RHS must fail", rawBinary(OpHASANY, "Tags", "A")},

		// IN expects scalar field, not slice field
		{"IN on slice field must fail", IN("Tags", []string{"A"})},

		// HAS/HASANY expect slice field, not scalar field
		{"HAS on scalar field must fail", HASALL("Age", []int{1})},
		{"HASANY on scalar field must fail", HASANY("Age", []int{1})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Match(u, tc.expr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestMatch_HASSliceElementTypeMismatchErrors(t *testing.T) {
	type S struct {
		IDs   []int
		Tags  []string
		Names []*string
	}

	admin := "admin"
	v := S{
		IDs:   []int{1, 2, 3},
		Tags:  []string{"admin", "editor"},
		Names: []*string{&admin},
	}

	cases := []struct {
		name string
		expr Expr
	}{
		{"HASANY mixed incompatible element must fail", HASANY("IDs", []any{1, "x"})},
		{"HASALL mixed incompatible element must fail", HASALL("IDs", []any{1, "x"})},
		{"HASANY invalid string slice element must fail", HASANY("Tags", []any{"admin", 1})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Match(v, tc.expr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}

	ok, err := Match(v, HASANY("Names", []string{"admin"}))
	if err != nil {
		t.Fatalf("unexpected error for pointer element comparison: %v", err)
	}
	if !ok {
		t.Fatalf("expected pointer slice element to compare against scalar query")
	}
}

func TestMatch_LiteralTypeCoercions(t *testing.T) {
	type S struct {
		ID   int
		U    uint64
		Name string
		F32  float32
	}

	v := S{
		ID:   1,
		U:    7,
		Name: "A",
		F32:  1.5,
	}

	cases := []struct {
		name string
		expr Expr
	}{
		{"negative to uint must fail", EQ("U", -1)},
		{"int to string must fail", EQ("Name", 65)},
		{"lossy float64 to float32 must fail", EQ("F32", 1.1)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Match(v, tc.expr)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}

	ok, err := Match(v, EQ("F32", 1.5))
	if err != nil {
		t.Fatalf("unexpected error for exact float conversion: %v", err)
	}
	if !ok {
		t.Fatalf("expected exact float64 literal to match float32 field")
	}

	ok, err = Match(v, EQ("ID", 1.0))
	if err != nil {
		t.Fatalf("unexpected error for exact float-to-int equality: %v", err)
	}
	if !ok {
		t.Fatalf("expected exact integer-valued float to match int field")
	}

	ok, err = Match(v, EQ("ID", 1.9))
	if err != nil {
		t.Fatalf("unexpected error for non-integral float-to-int equality: %v", err)
	}
	if ok {
		t.Fatalf("expected non-integral float to compare unequal to int field")
	}

	ok, err = Match(v, GT("ID", 1.9))
	if err != nil {
		t.Fatalf("unexpected error for float-to-int ordered comparison: %v", err)
	}
	if ok {
		t.Fatalf("expected 1 > 1.9 to be false")
	}

	ok, err = Match(S{ID: 2, U: 7, Name: "A", F32: 1.5}, GT("ID", 1.9))
	if err != nil {
		t.Fatalf("unexpected error for float-to-int ordered comparison: %v", err)
	}
	if !ok {
		t.Fatalf("expected 2 > 1.9 to be true")
	}

	ok, err = Match(v, GT("F32", 1))
	if err != nil {
		t.Fatalf("unexpected error for int-to-float ordered comparison: %v", err)
	}
	if !ok {
		t.Fatalf("expected float field to compare against int query without lossy cast")
	}

	ok, err = Match(v, IN("ID", []any{1.9, 1}))
	if err != nil {
		t.Fatalf("unexpected error for IN with cross numeric elements: %v", err)
	}
	if !ok {
		t.Fatalf("expected IN to succeed when one cross-type numeric arm matches exactly")
	}

	ok, err = Match(S{ID: 2, U: 7, Name: "A", F32: 1.5}, IN("ID", []any{1.9, 1}))
	if err != nil {
		t.Fatalf("unexpected error for IN with non-matching cross numeric elements: %v", err)
	}
	if ok {
		t.Fatalf("expected IN to stay false when no arm matches")
	}

	nan32 := float32(math.NaN())
	ok, err = Match(S{F32: nan32}, EQ("F32", math.NaN()))
	if err != nil {
		t.Fatalf("unexpected error for NaN float conversion: %v", err)
	}
	if ok {
		t.Fatalf("expected NaN equality to remain false after successful coercion")
	}
}

func TestMatch_NumericCrossTypeExactness(t *testing.T) {
	type S struct {
		I int64
		F float64
	}

	v := S{
		I: 2,
		F: float64(1 << 53),
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"int EQ exact float", EQ("I", 2.0), true},
		{"int EQ fractional float", EQ("I", 2.5), false},
		{"int GT fractional float", GT("I", 1.9), true},
		{"int LT fractional float", LT("I", 2.5), true},
		{"float EQ exact large int", EQ("F", int64(1<<53)), true},
		{"float EQ adjacent large int", EQ("F", int64((1<<53)+1)), false},
		{"float LT adjacent large int", LT("F", int64((1<<53)+1)), true},
		{"float GT previous large int", GT("F", int64((1<<53)-1)), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.ok {
				t.Fatalf("Match()=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestMatch_NilInputs(t *testing.T) {
	ok, err := Match(nil, EQ("ID", 1))
	if err != nil {
		t.Fatalf("Match(nil) must not error, got: %v", err)
	}
	if ok {
		t.Fatalf("Match(nil) must be false")
	}

	fn, err := CompileFor[User](EQ("ID", 1))
	if err != nil {
		t.Fatalf("CompileFor error: %v", err)
	}
	ok, err = fn(nil)
	if err != nil || ok {
		t.Fatalf("fn(nil) expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestMatch_StringOps(t *testing.T) {
	type S struct {
		Name   string
		Nick   *string
		AnyStr any
	}

	nick := "superuser"
	v := S{
		Name:   "Alice",
		Nick:   &nick,
		AnyStr: "hello world",
	}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"PREFIX true", PREFIX("Name", "Al"), true},
		{"PREFIX false", PREFIX("Name", "xx"), false},

		{"SUFFIX true", SUFFIX("Name", "ice"), true},
		{"SUFFIX false", SUFFIX("Name", "xx"), false},

		{"CONTAINS true", CONTAINS("Name", "li"), true},
		{"CONTAINS false", CONTAINS("Name", "zz"), false},

		{"PTR PREFIX true", PREFIX("Nick", "sup"), true},
		{"PTR SUFFIX true", SUFFIX("Nick", "user"), true},
		{"PTR CONTAINS true", CONTAINS("Nick", "peru"), true},

		{"ANY CONTAINS true", CONTAINS("AnyStr", "world"), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("Match()=%v want=%v", ok, tc.ok)
			}
		})
	}
}

func TestMatch_HASNONE(t *testing.T) {
	type S struct {
		Tags []string
	}

	v := S{Tags: []string{"A", "B", "C"}}

	cases := []struct {
		name string
		expr Expr
		ok   bool
	}{
		{"HASNONE true", HASNONE("Tags", []string{"X", "Y"}), true},
		{"HASNONE false", HASNONE("Tags", []string{"X", "B"}), false},
		{"HASNONE empty RHS => true", HASNONE("Tags", []string{}), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := Match(v, tc.expr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.ok {
				t.Fatalf("Match()=%v want=%v", ok, tc.ok)
			}
		})
	}
}

func TestMatch_EmbeddedAndAliases(t *testing.T) {
	type Base struct {
		BaseID int `json:"base_id"`
	}
	type Child struct {
		Base
		Name string `json:"name"`
	}

	c := Child{Base: Base{BaseID: 777}, Name: "Child"}

	ok, err := Match(c, EQ("BaseID", 777))
	if err != nil || !ok {
		t.Fatalf("expected match by promoted field, ok=%v err=%v", ok, err)
	}

	ok, err = Match(c, EQ("Base", Base{BaseID: 777}))
	if err != nil || !ok {
		t.Fatalf("expected match by embedded field name, ok=%v err=%v", ok, err)
	}

	ok, err = Match(c, EQ("base_id", 777))
	if err != nil || !ok {
		t.Fatalf("expected match by json alias, ok=%v err=%v", ok, err)
	}
}

func TestMatch_RepeatedAliasesSameField_AreAllowed(t *testing.T) {
	type S struct {
		ID int `json:"id" db:"id"`
		X  int `json:"X" db:"X"`
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	ok, err := m.Match(S{ID: 7, X: 9}, AND(EQ("id", 7), EQ("X", 9)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected repeated aliases for same field to remain usable")
	}
}

func TestMatch_EmbeddedNameConflict_PrefersPromotedField(t *testing.T) {
	type Base struct {
		Base int
		ID   int
	}
	type Child struct {
		Base
	}

	c := Child{Base: Base{Base: 7, ID: 11}}

	ok, err := Match(c, EQ("Base", 7))
	if err != nil || !ok {
		t.Fatalf("expected promoted child field to win, ok=%v err=%v", ok, err)
	}

	ok, err = Match(c, EQ("ID", 11))
	if err != nil || !ok {
		t.Fatalf("expected other promoted field to remain accessible, ok=%v err=%v", ok, err)
	}
}

func TestMatch_ShallowerFieldShadowsPromotedField(t *testing.T) {
	type Inner struct {
		X int
	}
	type S struct {
		X int
		Inner
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	v := S{
		X:     1,
		Inner: Inner{X: 2},
	}

	ok, err := m.Match(v, EQ("X", 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected direct field to shadow promoted field")
	}

	ok, err = m.Match(v, EQ("X", 2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected promoted field not to override direct field")
	}
}

func TestMatch_EmbeddedNameConflict_StillRejectsAmbiguity(t *testing.T) {
	type Base struct {
		Base int
	}
	type Other struct {
		Base int
	}
	type S struct {
		Base
		Other
	}

	_, err := NewFor[S]()
	if err == nil {
		t.Fatalf("expected error due to ambiguous promoted Base field")
	}
}

func TestMatch_AnonymousUnexportedEmbed_PromotesExportedFields(t *testing.T) {
	type inner struct {
		X int
	}
	type outer struct {
		inner
		Y int
	}

	m, err := NewFor[outer]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	v := outer{
		inner: inner{X: 11},
		Y:     5,
	}

	ok, err := m.Match(v, EQ("X", 11))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected promoted exported field from unexported anonymous embed to be addressable")
	}

	got, err := m.DiffFields(
		outer{inner: inner{X: 11}, Y: 1},
		outer{inner: inner{X: 12}, Y: 1},
	)
	if err != nil {
		t.Fatalf("unexpected diff error: %v", err)
	}
	if len(got) != 1 || got[0] != "X" {
		t.Fatalf("expected diff on promoted exported field X, got %v", got)
	}
}

func TestMatch_FastVsSlowPathConsistency(t *testing.T) {

	// behavior should not depend on whether the root value is passed by value or pointer

	type S struct {
		A int
		B string
		C float64
		D bool
	}

	s := S{A: 10, B: "fast", C: 3.14, D: true}

	exps := []Expr{
		EQ("A", 10),
		EQ("B", "fast"),
		GT("C", 3.0),
		EQ("D", true),
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	for _, e := range exps {
		fn, err := m.Compile(e)
		if err != nil {
			t.Fatalf("compile error for %v: %v", e, err)
		}

		ok1, err1 := fn(&s)
		ok2, err2 := fn(s)

		if err1 != nil || err2 != nil {
			t.Fatalf("unexpected errors: err1=%v err2=%v", err1, err2)
		}
		if ok1 != ok2 {
			t.Fatalf("fast/slow mismatch for %v: ptr=%v val=%v", e, ok1, ok2)
		}
	}
}

func TestMatch_FastVsSlowPathConsistency_SliceOps(t *testing.T) {
	type S struct {
		Tags    []string
		TagsPtr *[]string
	}

	tags := []string{"A", "B", "C"}
	s := S{
		Tags:    tags,
		TagsPtr: &tags,
	}

	exps := []Expr{
		HASALL("Tags", []string{"A", "C"}),
		HASANY("Tags", []string{"X", "B"}),
		HASALL("TagsPtr", []string{"A", "B"}),
		HASANY("TagsPtr", []string{"Y", "C"}),
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	for _, e := range exps {
		fn, err := m.Compile(e)
		if err != nil {
			t.Fatalf("compile error for %v: %v", e, err)
		}

		ok1, err1 := fn(&s)
		ok2, err2 := fn(s)

		if err1 != nil || err2 != nil {
			t.Fatalf("unexpected errors: err1=%v err2=%v", err1, err2)
		}
		if ok1 != ok2 {
			t.Fatalf("fast/slow mismatch for %v: ptr=%v val=%v", e, ok1, ok2)
		}
	}
}

func TestAliasCollisions_MustFail(t *testing.T) {

	// alias collisions must be rejected (fail-fast)

	type Bad struct {
		Field1 int `db:"field_1" json:"-"`
		Field2 any `db:"-" json:"field_1"`
	}

	_, err := CompileFor[Bad](EQ("field_1", 1))
	if err == nil {
		t.Fatalf("expected error due to alias collision (field_1), got nil")
	}
}

func TestRace_ConcurrentCompileAndMatch(t *testing.T) {

	t.Parallel()

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	// shared data for all goroutines

	data := []*S{
		{
			ID:       777,
			Age:      35,
			Name:     "Alice",
			Score:    88.8,
			IsActive: true,
			RolePtr:  strPtr("superuser"),
			Tags:     []string{"admin", "editor", "A", "B"},
			Meta:     intPtr(123),
			Profile:  Profile{Level: 5, Rank: intPtr(99)},
		},
		{
			ID:       1,
			Age:      20,
			Name:     "Bob",
			Score:    12.3,
			IsActive: false,
			RolePtr:  nil,
			Tags:     []string{"user"},
			Meta:     0,
			Profile:  Profile{Level: 1, Rank: nil},
		},
	}

	// expression intentionally touches:
	// - fast scalars
	// - slice ops
	// - interface unwrap

	expr := AND(
		GTE("Age", 18),
		LT("Score", 100.0),
		OR(
			EQ("IsActive", true),
			EQ("Name", "Bob"),
		),
		HASANY("Tags", []string{"admin", "B"}),
		EQ("Meta", 123),
	)

	const workers = 16
	const iters = 100000

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()

			for i := 0; i < iters; i++ {

				// compile concurrently as well (hits caches, reflection, etc.)

				m, err := NewFor[S]()
				if err != nil {
					t.Errorf("NewFor error: %v", err)
					return
				}
				fn, err := m.Compile(expr)
				if err != nil {
					t.Errorf("compile error: %v", err)
					return
				}

				// run matches on shared objects

				ok0, err0 := fn(data[0])
				if err0 != nil {
					t.Errorf("match error: %v", err0)
					return
				}

				// data[0] should match
				if !ok0 {
					t.Errorf("expected match for data[0], got false")
					return
				}

				ok1, err1 := fn(data[1])
				if err1 != nil {
					t.Errorf("match error: %v", err1)
					return
				}
				// data[1] should not match
				if ok1 {
					t.Errorf("expected no match for data[1], got true")
					return
				}
			}
		}()
	}

	wg.Wait()
}

/**/

func toSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

func mustContainAll(t *testing.T, got []string, want []string) {
	t.Helper()
	g := toSet(got)
	for _, w := range want {
		if _, ok := g[w]; !ok {
			t.Fatalf("missing \"%v\" in %v", w, got)
		}
	}
}

func TestMatcher_DiffFields_TwoValues(t *testing.T) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int    `json:"id" db:"pk"`
		Name     string `json:"name"`
		Age      int    `json:"age"`
		IsActive bool
		Tags     []string
		Meta     any
		Profile  Profile `json:"profile"`
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	a := &S{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		IsActive: true,
		Tags:     []string{"A", "B"},
		Meta:     intPtr(123),
		Profile:  Profile{Level: 5, Rank: intPtr(99)},
	}
	b := &S{
		ID:       1,                                   // same
		Name:     "Alice",                             // same
		Age:      31,                                  // diff
		IsActive: false,                               // diff
		Tags:     []string{"A", "B"},                  // same
		Meta:     123,                                 // same by data equality (unwrap)
		Profile:  Profile{Level: 5, Rank: intPtr(99)}, // same by data equality (deep)
	}

	got, err := m.DiffFields(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"Age", "IsActive"}
	mustContainAll(t, got, want)

	gotSet := toSet(got)
	if _, ok := gotSet["ID"]; ok {
		t.Fatalf("ID should not be in diff: %v", got)
	}
	if _, ok := gotSet["Name"]; ok {
		t.Fatalf("Name should not be in diff: %v", got)
	}
	if _, ok := gotSet["Meta"]; ok {
		t.Fatalf("Meta should not be in diff (data-equality expected): %v", got)
	}
	if _, ok := gotSet["Profile"]; ok {
		t.Fatalf("Profile should not be in diff (data-equality expected): %v", got)
	}
}

func TestMatcher_DiffFields_ThreeValues(t *testing.T) {
	type S struct {
		A int `json:"a"`
		B int `json:"b"`
		C int `json:"c"`
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	v1 := &S{A: 1, B: 2, C: 3}
	v2 := &S{A: 1, B: 999, C: 3}
	v3 := &S{A: 1, B: 2, C: 777}

	got, err := m.DiffFields(v1, v2, v3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// B differs vs v2, C differs vs v3
	// A same across all

	sort.Strings(got)
	want := []string{"B", "C"}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("DiffFields mismatch: got=%v want=%v", got, want)
	}
}

func TestMatcher_DiffFieldsTag_JSON(t *testing.T) {
	type S struct {
		ID       int    `json:"id" db:"pk"`
		Name     string `json:"name"`
		Age      int    `json:"age"`
		IsActive bool   // no json tag -> fallback to field name
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	a := &S{ID: 1, Name: "Alice", Age: 30, IsActive: true}
	b := &S{ID: 2, Name: "Alice", Age: 31, IsActive: false}

	got, err := m.DiffFieldsTag("json", a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// expect tag names for tagged fields, fallback to field name for untagged
	want := []string{"id", "age", "IsActive"}
	mustContainAll(t, got, want)

	// ensure it does not return the Go field name for tagged ones
	gotSet := toSet(got)
	if _, ok := gotSet["ID"]; ok {
		t.Fatalf("expected json tag 'id' instead of 'ID': %v", got)
	}
	if _, ok := gotSet["Age"]; ok {
		t.Fatalf("expected json tag 'age' instead of 'Age': %v", got)
	}
}

func TestMatcher_DiffFieldsTag_JSON_HiddenPromotedAlias(t *testing.T) {
	type Inner struct {
		X int `json:"inner_x"`
	}
	type S struct {
		X int `json:"x"`
		Inner
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	a := S{X: 1, Inner: Inner{X: 10}}
	b := S{X: 1, Inner: Inner{X: 11}}

	got, err := m.DiffFieldsTag("json", a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotSet := toSet(got)
	if _, ok := gotSet["inner_x"]; !ok {
		t.Fatalf("expected json alias for hidden promoted field, got %v", got)
	}
}

func TestMatcher_DiffFieldsTag_CustomTag(t *testing.T) {
	type S struct {
		ID   int `xml:"id"`
		Name int `xml:"name"`
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	a := S{ID: 1, Name: 10}
	b := S{ID: 2, Name: 10}

	got, err := m.DiffFieldsTag("xml", a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got) != 1 || got[0] != "id" {
		t.Fatalf("expected custom tag name, got %v", got)
	}
}

func TestMatcher_DiffFields_TypeMismatchErrors(t *testing.T) {
	type S struct{ A int }

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	_, err = m.DiffFields(&S{A: 1}, struct{ A int64 }{A: 1})
	if err == nil {
		t.Fatalf("expected error on type mismatch")
	}
}

func TestMatcher_DiffFields_AcyclicCompositeDataEquality(t *testing.T) {
	type Child struct {
		Rank int
	}
	type Payload struct {
		Labels []string
		Scores map[string]*int
		Meta   any
		Child  *Child
	}
	type S struct {
		Payload Payload
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	a := S{
		Payload: Payload{
			Labels: []string{"A", "B"},
			Scores: map[string]*int{
				"one": intPtr(1),
				"two": intPtr(2),
			},
			Meta:  []int{7, 8, 9},
			Child: &Child{Rank: 5},
		},
	}
	b := S{
		Payload: Payload{
			Labels: []string{"A", "B"},
			Scores: map[string]*int{
				"one": intPtr(1),
				"two": intPtr(2),
			},
			Meta:  []int{7, 8, 9},
			Child: &Child{Rank: 5},
		},
	}

	got, err := m.DiffFields(a, b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no diff for acyclic composite data equality, got %v", got)
	}
}

func TestMatcher_DiffFields_InterfaceValues_StrictTypes(t *testing.T) {
	type S struct {
		Meta any
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	got, err := m.DiffFields(S{Meta: -1}, S{Meta: ^uint64(0)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "Meta" {
		t.Fatalf("expected Meta to differ, got %v", got)
	}
}

func TestMatcher_DiffFields_FuncNilOnlySemantics(t *testing.T) {
	type S struct {
		Fn func()
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	got, err := m.DiffFields(S{}, S{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected nil funcs to compare equal, got %v", got)
	}

	fn := func() {}
	got, err = m.DiffFields(S{Fn: fn}, S{Fn: fn})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "Fn" {
		t.Fatalf("expected same non-nil func values to differ, got %v", got)
	}
}

func TestMatcher_DiffFields_PointerNaNSemantics(t *testing.T) {
	type S struct {
		Rank *float64
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}

	n := math.NaN()
	p := &n

	got, err := m.DiffFields(&S{Rank: p}, &S{Rank: p})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got) != 1 || got[0] != "Rank" {
		t.Fatalf("expected Rank to differ for same pointer-to-NaN, got %v", got)
	}
}

func TestMatch_Allocations_ScalarCompiledPointerHotPath(t *testing.T) {
	type S struct {
		ID int
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(EQ("ID", 42))
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}

	v := &S{ID: 42}
	allocs := testing.AllocsPerRun(1000, func() {
		ok, err := fn(v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		benchSinkBool = ok
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs, got %.2f", allocs)
	}
	if !benchSinkBool {
		t.Fatalf("expected match")
	}
}

func TestMatch_Allocations_MixedCompiledPointerHotPath(t *testing.T) {
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		Tags     []string
		Meta     any
	}

	m, err := NewFor[S]()
	if err != nil {
		t.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(AND(
		EQ("ID", 42),
		GTE("Age", 18),
		LT("Score", 100.0),
		EQ("IsActive", true),
		IN("Name", []string{"Bob", "Alice", "Charlie"}),
		HASANY("Tags", []string{"Z", "B"}),
		EQ("Meta", 123),
	))
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}

	v := &S{
		ID:       42,
		Age:      30,
		Name:     "Alice",
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"A", "B", "C"},
		Meta:     intPtr(123),
	}

	allocs := testing.AllocsPerRun(1000, func() {
		ok, err := fn(v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		benchSinkBool = ok
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs, got %.2f", allocs)
	}
	if !benchSinkBool {
		t.Fatalf("expected match")
	}
}

/**/

var (
	benchSinkBool bool
	benchSinkErr  error
	benchSinkInt  int
)

func BenchmarkMatch_Small(b *testing.B) {

	// simple: few scalar filters, everything should be fast-path

	type S struct {
		A int
		B string
		C float64
		D bool
		E uint64
	}

	v := &S{
		A: 10,
		B: "ok",
		C: 3.14,
		D: true,
		E: 123,
	}

	expr := AND(
		EQ("A", 10),
		EQ("B", "ok"),
		GT("C", 3.0),
		EQ("D", true),
		LT("E", uint64(200)),
	)

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_Small(b *testing.B) {
	type S struct {
		A int
		B string
		C float64
		D bool
		E uint64
	}

	v := &S{
		A: 10,
		B: "ok",
		C: 3.14,
		D: true,
		E: 123,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok := v.A == 10 &&
			v.B == "ok" &&
			v.C > 3.0 &&
			v.D &&
			v.E < 200
		benchSinkBool = ok
	}
}

func BenchmarkMatch_Mixed(b *testing.B) {

	// mixed: some fast scalar checks + slower bits (interface / slice ops)

	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		Tags     []string
		Meta     any // forces slow-ish comparisons
	}

	v := &S{
		ID:       42,
		Age:      30,
		Name:     "Alice",
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"A", "B", "C"},
		Meta:     intPtr(123), // interface holding pointer
	}

	expr := AND(

		EQ("ID", 42),
		GTE("Age", 18),
		LT("Score", 100.0),
		EQ("IsActive", true),

		IN("Name", []string{"Bob", "Alice", "Charlie"}),
		HASANY("Tags", []string{"Z", "B"}),
		EQ("Meta", 123),
	)

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_Mixed(b *testing.B) {
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		Tags     []string
		Meta     any
	}

	v := &S{
		ID:       42,
		Age:      30,
		Name:     "Alice",
		Score:    95.5,
		IsActive: true,
		Tags:     []string{"A", "B", "C"},
		Meta:     intPtr(123),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hasAnyTags := false
		for _, tag := range v.Tags {
			if tag == "Z" || tag == "B" {
				hasAnyTags = true
				break
			}
		}

		metaEq123 := false
		switch mv := v.Meta.(type) {
		case int:
			metaEq123 = mv == 123
		case *int:
			metaEq123 = mv != nil && *mv == 123
		}

		ok := v.ID == 42 &&
			v.Age >= 18 &&
			v.Score < 100.0 &&
			v.IsActive &&
			(v.Name == "Bob" || v.Name == "Alice" || v.Name == "Charlie") &&
			hasAnyTags &&
			metaEq123

		benchSinkBool = ok
	}
}

func BenchmarkMatch_Heavy(b *testing.B) {

	// heavy: large expression + more checks (still using *T)

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	v := &S{
		ID:       777,
		Age:      35,
		Name:     "Alice",
		Score:    88.8,
		IsActive: true,
		RolePtr:  strPtr("superuser"),
		Tags:     []string{"admin", "editor", "A", "B"},
		Meta:     intPtr(123),
		Profile: Profile{
			Level: 5,
			Rank:  intPtr(99),
		},
	}

	// also include an EQ on nested struct (slow path / recursive-aware data equality), and a NE on nested struct

	profSame := Profile{Level: 5, Rank: intPtr(99)}
	profOther := Profile{Level: 6, Rank: intPtr(99)}

	expr := AND(
		OR(
			AND(EQ("ID", 777), EQ("Name", "Alice"), GTE("Age", 18)),
			AND(GT("Age", 40), EQ("IsActive", true), LT("Score", 90.0)),
			AND(EQ("RolePtr", "superuser"), NOT(EQ("Name", "Bob"))),
		),

		AND(
			LT("Score", 100.0),
			GTE("Age", 21),
			EQ("IsActive", true),
			NOT(EQ("ID", 0)),
		),

		AND(
			HASALL("Tags", []string{"admin", "editor"}),
			HASANY("Tags", []string{"Z", "B"}),
			IN("Name", []string{"Alice", "Charlie", "Dave", "Eve"}),
		),

		AND(
			EQ("Meta", 123),
			NOT(EQ("Meta", 999)),
		),

		AND(
			EQ("Profile", profSame),
			NE("Profile", profOther),
		),
	)

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		if e != nil {
			panic(e)
		}
		if !ok {
			panic("aaa")
		}
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_Heavy(b *testing.B) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	v := &S{
		ID:       777,
		Age:      35,
		Name:     "Alice",
		Score:    88.8,
		IsActive: true,
		RolePtr:  strPtr("superuser"),
		Tags:     []string{"admin", "editor", "A", "B"},
		Meta:     intPtr(123),
		Profile: Profile{
			Level: 5,
			Rank:  intPtr(99),
		},
	}

	profSame := Profile{Level: 5, Rank: intPtr(99)}
	profOther := Profile{Level: 6, Rank: intPtr(99)}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hasAdmin := false
		hasEditor := false
		hasAnyTags := false
		for _, tag := range v.Tags {
			if tag == "admin" {
				hasAdmin = true
			}
			if tag == "editor" {
				hasEditor = true
			}
			if tag == "Z" || tag == "B" {
				hasAnyTags = true
			}
			if hasAdmin && hasEditor && hasAnyTags {
				break
			}
		}

		metaEq123 := false
		metaEq999 := false
		switch mv := v.Meta.(type) {
		case int:
			metaEq123 = mv == 123
			metaEq999 = mv == 999
		case *int:
			if mv != nil {
				metaEq123 = *mv == 123
				metaEq999 = *mv == 999
			}
		}

		profileEqSame := v.Profile.Level == profSame.Level
		if profileEqSame {
			switch {
			case v.Profile.Rank == nil || profSame.Rank == nil:
				profileEqSame = v.Profile.Rank == profSame.Rank
			default:
				profileEqSame = *v.Profile.Rank == *profSame.Rank
			}
		}

		profileEqOther := v.Profile.Level == profOther.Level
		if profileEqOther {
			switch {
			case v.Profile.Rank == nil || profOther.Rank == nil:
				profileEqOther = v.Profile.Rank == profOther.Rank
			default:
				profileEqOther = *v.Profile.Rank == *profOther.Rank
			}
		}

		leftOR :=
			(v.ID == 777 && v.Name == "Alice" && v.Age >= 18) ||
				(v.Age > 40 && v.IsActive && v.Score < 90.0) ||
				(v.RolePtr != nil && *v.RolePtr == "superuser" && v.Name != "Bob")

		ok := leftOR &&
			(v.Score < 100.0 && v.Age >= 21 && v.IsActive && v.ID != 0) &&
			(hasAdmin && hasEditor && hasAnyTags && (v.Name == "Alice" || v.Name == "Charlie" || v.Name == "Dave" || v.Name == "Eve")) &&
			(metaEq123 && !metaEq999) &&
			(profileEqSame && !profileEqOther)

		if !ok {
			panic("aaa")
		}
		benchSinkBool = ok
	}
}

func BenchmarkMatch_MixedLoop1000(b *testing.B) {

	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	// xorshift
	var x uint64 = 0x123456789abcdef
	next := func() uint64 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return x
	}

	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve"}
	tagPool := []string{"admin", "editor", "user", "A", "B", "C", "X", "Y", "Z"}
	rolePool := []string{"superuser", "staff", "guest"}

	const N = 1000
	data := make([]*S, 0, N)

	for i := 0; i < N; i++ {
		r1 := next()
		r2 := next()

		age := int(r1%70) + 10
		score := float64(r2%10000) / 100.0 // 0..100
		isActive := r1&1 == 0

		name := names[int((r1>>8)%uint64(len(names)))]

		// small tags slice (0..3 items) — realistic, not huge
		tagCount := int((r2 >> 8) % 4)
		tags := make([]string, 0, tagCount)
		for j := 0; j < tagCount; j++ {
			tags = append(tags, tagPool[int((next()>>16)%uint64(len(tagPool)))])
		}

		// RolePtr nil sometimes
		var rolePtr *string
		if (r2 & 3) != 0 {
			s := rolePool[int((r2>>20)%uint64(len(rolePool)))]
			rolePtr = &s
		}

		// Meta sometimes int, sometimes *int
		var meta any
		mv := int((r1 >> 32) % 500)
		if (r1 & 4) == 0 {
			meta = mv
		} else {
			meta = intPtr(mv)
		}

		// Profile (not used in expr; just for shape realism)
		var rank *int
		if (r1 & 8) != 0 {
			rank = intPtr(int((r2 >> 32) % 200))
		}

		data = append(data, &S{
			ID:       i + 1,
			Age:      age,
			Name:     name,
			Score:    score,
			IsActive: isActive,
			RolePtr:  rolePtr,
			Tags:     tags,
			Meta:     meta,
			Profile:  Profile{Level: int((r1 >> 40) % 10), Rank: rank},
		})
	}

	// mix of fast scalars + slice ops + interface unwrap
	// targeting a modest match rate (not too high/low)

	expr := AND(
		GTE("Age", 18),
		LT("Score", 90.0),
		EQ("IsActive", true),
		IN("Name", []string{"Alice", "Bob"}),
		HASANY("Tags", []string{"admin", "editor", "B"}),
		EQ("Meta", 123),
	)

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(expr)
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cnt := 0
		for _, v := range data {
			ok, e := fn(v)
			if e != nil {
				benchSinkErr = e
				b.Fatalf("unexpected error: %v", e)
			}
			if ok {
				cnt++
			}
		}
		benchSinkInt = cnt
	}
}

func BenchmarkNative_MixedLoop1000(b *testing.B) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Age      int
		Name     string
		Score    float64
		IsActive bool
		RolePtr  *string
		Tags     []string
		Meta     any
		Profile  Profile
	}

	var x uint64 = 0x123456789abcdef
	next := func() uint64 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return x
	}

	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve"}
	tagPool := []string{"admin", "editor", "user", "A", "B", "C", "X", "Y", "Z"}
	rolePool := []string{"superuser", "staff", "guest"}

	const N = 1000
	data := make([]*S, 0, N)

	for i := 0; i < N; i++ {
		r1 := next()
		r2 := next()

		age := int(r1%70) + 10
		score := float64(r2%10000) / 100.0
		isActive := r1&1 == 0
		name := names[int((r1>>8)%uint64(len(names)))]

		tagCount := int((r2 >> 8) % 4)
		tags := make([]string, 0, tagCount)
		for j := 0; j < tagCount; j++ {
			tags = append(tags, tagPool[int((next()>>16)%uint64(len(tagPool)))])
		}

		var rolePtr *string
		if (r2 & 3) != 0 {
			s := rolePool[int((r2>>20)%uint64(len(rolePool)))]
			rolePtr = &s
		}

		var meta any
		mv := int((r1 >> 32) % 500)
		if (r1 & 4) == 0 {
			meta = mv
		} else {
			meta = intPtr(mv)
		}

		var rank *int
		if (r1 & 8) != 0 {
			rank = intPtr(int((r2 >> 32) % 200))
		}

		data = append(data, &S{
			ID:       i + 1,
			Age:      age,
			Name:     name,
			Score:    score,
			IsActive: isActive,
			RolePtr:  rolePtr,
			Tags:     tags,
			Meta:     meta,
			Profile:  Profile{Level: int((r1 >> 40) % 10), Rank: rank},
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cnt := 0
		for _, v := range data {
			hasAnyTags := false
			for _, tag := range v.Tags {
				if tag == "admin" || tag == "editor" || tag == "B" {
					hasAnyTags = true
					break
				}
			}

			metaEq123 := false
			switch mv := v.Meta.(type) {
			case int:
				metaEq123 = mv == 123
			case *int:
				metaEq123 = mv != nil && *mv == 123
			}

			ok := v.Age >= 18 &&
				v.Score < 90.0 &&
				v.IsActive &&
				(v.Name == "Alice" || v.Name == "Bob") &&
				hasAnyTags &&
				metaEq123

			if ok {
				cnt++
			}
		}
		benchSinkInt = cnt
	}
}

func BenchmarkMatch_HasAnyStrings(b *testing.B) {
	type S struct {
		Tags []string
	}

	v := &S{
		Tags: []string{"admin", "editor", "A", "B"},
	}

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(HASANY("Tags", []string{"Z", "B", "editor"}))
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_HasAnyStrings(b *testing.B) {
	type S struct {
		Tags []string
	}

	v := &S{
		Tags: []string{"admin", "editor", "A", "B"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok := false
		for _, tag := range v.Tags {
			if tag == "Z" || tag == "B" || tag == "editor" {
				ok = true
				break
			}
		}
		benchSinkBool = ok
	}
}

func BenchmarkMatch_HasStrings(b *testing.B) {
	type S struct {
		Tags []string
	}

	v := &S{
		Tags: []string{"admin", "editor", "A", "B"},
	}

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(HASALL("Tags", []string{"admin", "editor"}))
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_HasStrings(b *testing.B) {
	type S struct {
		Tags []string
	}

	v := &S{
		Tags: []string{"admin", "editor", "A", "B"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hasAdmin := false
		hasEditor := false
		for _, tag := range v.Tags {
			if tag == "admin" {
				hasAdmin = true
			}
			if tag == "editor" {
				hasEditor = true
			}
			if hasAdmin && hasEditor {
				break
			}
		}
		benchSinkBool = hasAdmin && hasEditor
	}
}

func BenchmarkMatch_INInts(b *testing.B) {
	type S struct {
		ID int
	}

	v := &S{ID: 42}

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}
	fn, err := m.Compile(IN("ID", []int{7, 42, 100, 200}))
	if err != nil {
		b.Fatalf("compile error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, e := fn(v)
		benchSinkBool = ok
		benchSinkErr = e
	}
}

func BenchmarkNative_INInts(b *testing.B) {
	type S struct {
		ID int
	}

	v := &S{ID: 42}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok := v.ID == 7 || v.ID == 42 || v.ID == 100 || v.ID == 200
		benchSinkBool = ok
	}
}

/**/

func BenchmarkDiffFields_Overhead(b *testing.B) {
	type S struct {
		ID       int
		Age      int
		Score    float64
		IsActive bool
		Name     string
		Flag     uint64
	}

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}

	left := &S{ID: 1, Age: 30, Score: 88.8, IsActive: true, Name: "Alice", Flag: 10}
	right := &S{ID: 2, Age: 31, Score: 88.8, IsActive: false, Name: "Bob", Flag: 10}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		diff, err := m.DiffFields(left, right)
		if err != nil {
			benchSinkErr = err
			b.Fatalf("unexpected error: %v", err)
		}
		benchSinkInt = len(diff)
	}
}

func BenchmarkDiffFields_Heavy(b *testing.B) {
	type Profile struct {
		Level int
		Rank  *int
	}
	type S struct {
		ID       int
		Name     string
		Age      int
		IsActive bool
		Tags     []string
		Meta     any
		Profile  Profile
	}

	m, err := NewFor[S]()
	if err != nil {
		b.Fatalf("NewFor error: %v", err)
	}

	left := &S{
		ID:       1,
		Name:     "Alice",
		Age:      30,
		IsActive: true,
		Tags:     []string{"A", "B"},
		Meta:     intPtr(123),
		Profile:  Profile{Level: 5, Rank: intPtr(99)},
	}
	right := &S{
		ID:       2,
		Name:     "Bob",
		Age:      31,
		IsActive: false,
		Tags:     []string{"A", "B"},
		Meta:     123,
		Profile:  Profile{Level: 6, Rank: intPtr(99)},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		diff, err := m.DiffFields(left, right)
		if err != nil {
			benchSinkErr = err
			b.Fatalf("unexpected error: %v", err)
		}
		benchSinkInt = len(diff)
	}
}
