# smatch

[![GoDoc](https://godoc.org/github.com/vapstack/smatch?status.svg)](https://godoc.org/github.com/vapstack/smatch)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/smatch/master/LICENSE)

High-performance in-memory matcher.\
A fast evaluator that applies expressions to Go structs using reflection and unsafe optimizations.

Expressions are built using [qx](https://github.com/vapstack/qx) package.

Fields can be referenced by Go name and struct tags (`json`, `db`, ...).

### One-shot matching

For occasional checks:

```go
match, err := smatch.Match(user, qx.EQ("Age", 30))
```

or, using a matcher instance:

```go
m, err := smatch.NewFor[User]()
// ...
match, err := m.Match(user, qx.EQ("Age", 30))
```

### Compiled predicates (recommended)

For repeated evaluations of the same expression:

```go
m, err := smatch.NewFor[User]()
// ...
check, err := m.Compile(
    qx.AND(
        qx.GTE("age", 18),
        qx.EQ("active", true),
    )
)
// ...
for _, v := range values {
    match, err := check(v)
    // ...
}
```

Compiled predicates:

- avoid repeated expression traversal
- use fast paths when possible
- can be safely reused across calls

### Value handling

Matching APIs accept:

- struct values (T)
- pointers to structs (*T)
- interfaces wrapping either

Passing a pointer enables additional fast paths based on unsafe offsets.\
Passing `nil` to `Match`, `Matcher.Match`, or compiled predicates returns `(false, nil)`.

### Supported predicates

Matching API supports field-based predicates of the form 
`OP(REF(field), LIT(value))`,  plus logical composition.

- logical: `AND`, `OR`, `NOT`
- comparison: `EQ`, `NE`, `GT`, `GTE`, `LT`, `LTE`, `BETWEEN`
- membership: `IN`, `NOTIN`, `HASALL`, `HASANY`, `HASNONE`
- strings: `PREFIX`, `SUFFIX`, `CONTAINS`

### Equality semantics

For acyclic values, equality is defined mostly in terms of data:

- pointers are compared by the values they point to
- interfaces are unwrapped
- structs are compared field-by-field
- arrays and slices use element-wise comparison
- maps compare by exact key lookup and recursively compared values
- channels use identity semantics
- functions compare equal only when both sides are nil

Recursive type graphs fall back to `reflect.DeepEqual`.\
Maps with `NaN` keys are not a supported equality edge case.

### Cross-type rules

For a concrete field (`EQ/GT/GTE/LT/LTE` against a known field type), 
scalar comparisons allow exact numeric comparisons across `int*`, `uint*`, 
and `float*`, plus exact same-family conversions for `bool`, `string`, 
and `complex`. For `EQ` on non-scalars, Go-convertible values with the same 
underlying type are also accepted. Lossy scalar conversions and unrelated scalar 
families such as `bool` vs numeric or `string` vs numeric are rejected.

For `interface`/`any` fields in `Match`, cross-type support is narrower:
exact numeric comparisons across `int*`, `uint*`, and `float*`, 
plus same-family scalar equality for `bool`, `string`, and `complex`.
There is no general structural cross-type equality for `struct`, `slice`, 
`array`, `map`, `pointer`, `chan`, or `func` values stored in an interface.

`DiffFields` first normalizes root values to the matcher's source type, so it is
not a general "compare arbitrary struct types" API. The stricter rules apply to
fallback equality inside fields, especially `interface`/`any` values: same-type
values compare structurally, and cross-type equality is limited to scalar values
within the same family: `int*` with `int*`, `uint*` with `uint*`, `float*` with
`float*`, `complex*` with `complex*`, plus `bool` and `string`. There is no
`int <-> uint`, no `int/uint <-> float`, and no general structural cross-type
equality.

Examples:
- comparable: `int32(10)` vs `int64(10)`, `MyString("a")` vs `"a"`
- comparable in `Match`, but not in `DiffFields`: `int(10)` vs `float64(10)`
- not comparable across Go type boundaries: `[]byte("a")` vs `"a"`, `MyStruct{...}` vs
  `PlainStruct{...}` inside `interface`/`any`

### Performance

In general, matching is ~5-20x slower than a similar native comparison.

- No allocations on the fast path for scalar comparisons
- Optional unsafe field access when values are passed as `*T`
- Reflection metadata cached per type at package level
- Expression compilation amortizes cost over repeated runs

Rough estimates:

- Simple scalar predicates: tens of nanoseconds
- Mixed predicates: tens to hundreds of nanoseconds
- Complex structures (slices, nested structs): higher cost, still allocation-aware
