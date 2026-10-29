package sessionmanager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The Claude assay cannot ask the provider to constrain its own output.
//
// Schema-constrained output on claude 2.1.220 is implemented as a tool named
// StructuredOutput: probing --json-schema against the assay deny policy produced
// an init event reporting tools: ["StructuredOutput"], five tool_use attempts,
// and error_max_structured_output_retries. Passing the caller's schema would
// therefore both re-add a tool to a run whose entire claim is that it had none
// and prevent the run from completing at all.
//
// The caller's schema is consequently enforced here, on the returned answer,
// before anything is scored. That is only honest if the enforcement is total, so
// this validator has one governing rule: it either enforces every keyword the
// caller wrote or it refuses the assay. A schema containing a construct it
// cannot evaluate is reported as context_assay_isolation_unavailable rather than
// silently under-enforced, because a partially-checked answer that reaches the
// scorer is exactly the failure an "accepted and then ignored" schema produced.

// claudeAssaySchema is one compiled node of the enforceable JSON Schema subset.
//
// A nil pointer field means the keyword was absent and constrains nothing. A
// boolean schema (`true`/`false`) is kept as such because `additionalProperties:
// false` is how CV-2 forbids extra fact ids, and that has to be a first-class
// case rather than an approximation.
type claudeAssaySchema struct {
	boolean *bool

	types      []string
	enumValues []any
	constValue *any

	properties        map[string]*claudeAssaySchema
	required          []string
	additional        *claudeAssaySchema
	propertyNames     *claudeAssaySchema
	minProperties     *int
	maxProperties     *int
	dependentRequired map[string][]string

	items       *claudeAssaySchema
	minItems    *int
	maxItems    *int
	uniqueItems bool

	minLength  *int
	maxLength  *int
	pattern    *regexp.Regexp
	patternRaw string

	// The numeric assertions are exact rationals, not float64. A JSON number is a
	// decimal literal of unbounded precision, so collapsing one through binary64
	// makes distinct documents compare equal: 9007199254740993 and
	// 9007199254740992 are the same float64, and 0.1+0.2-style rounding makes
	// decimal divisibility answer wrongly. Either would let this adapter report
	// adapter-json-schema for an answer that does not conform.
	minimum          *big.Rat
	maximum          *big.Rat
	exclusiveMinimum *big.Rat
	exclusiveMaximum *big.Rat
	multipleOf       *big.Rat

	allOf []*claudeAssaySchema
	anyOf []*claudeAssaySchema
	oneOf []*claudeAssaySchema
	not   *claudeAssaySchema

	ifSchema   *claudeAssaySchema
	thenSchema *claudeAssaySchema
	elseSchema *claudeAssaySchema
}

// claudeAssayAnnotationKeywords carry no assertion, so ignoring them under-
// enforces nothing.
var claudeAssayAnnotationKeywords = map[string]bool{
	"$schema":     true,
	"$id":         true,
	"$comment":    true,
	"title":       true,
	"description": true,
	"default":     true,
	"examples":    true,
	"deprecated":  true,
	"readOnly":    true,
	"writeOnly":   true,
}

// compileClaudeAssaySchema prepares a caller schema for enforcement, or reports
// exactly which construct makes enforcement impossible.
//
// The returned error is not a validation failure — it says this adapter cannot
// stand behind the schema, which is a capability statement and is surfaced as
// such by the caller.
func compileClaudeAssaySchema(raw json.RawMessage) (*claudeAssaySchema, error) {
	decoded, err := claudeDecodeJSONValue(raw)
	if err != nil {
		return nil, fmt.Errorf("the assay output schema is not valid JSON: %w", err)
	}
	// Screened whole-document rather than per-keyword: const and enum carry
	// arbitrary values, so a literal this adapter cannot compare exactly can sit
	// anywhere in the schema, not only on a bound.
	if err := claudeRequireExactNumbers("$", decoded); err != nil {
		return nil, err
	}
	return compileClaudeAssayNode("$", decoded)
}

func compileClaudeAssayNode(pointer string, node any) (*claudeAssaySchema, error) {
	if flag, ok := node.(bool); ok {
		return &claudeAssaySchema{boolean: &flag}, nil
	}
	object, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema at %s must be an object or a boolean", pointer)
	}
	schema := &claudeAssaySchema{}
	// Sorted so an unsupported-keyword refusal names the same keyword on every
	// run; Go map iteration order would otherwise make the message unstable.
	for _, key := range claudeSortedKeys(object) {
		value := object[key]
		var err error
		switch key {
		case "type":
			schema.types, err = claudeSchemaTypeNames(pointer, value)
		case "enum":
			schema.enumValues, err = claudeSchemaArray(pointer, key, value)
			if err == nil && len(schema.enumValues) == 0 {
				err = fmt.Errorf("enum at %s is empty, so it can never be satisfied", pointer)
			}
		case "const":
			constant := value
			schema.constValue = &constant
		case "properties":
			schema.properties, err = claudeSchemaMap(pointer, key, value)
		case "required":
			schema.required, err = claudeSchemaStrings(pointer, key, value)
		case "additionalProperties":
			schema.additional, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "propertyNames":
			schema.propertyNames, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "minProperties":
			schema.minProperties, err = claudeSchemaInt(pointer, key, value)
		case "maxProperties":
			schema.maxProperties, err = claudeSchemaInt(pointer, key, value)
		case "dependentRequired":
			schema.dependentRequired, err = claudeSchemaStringLists(pointer, key, value)
		case "items":
			schema.items, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "minItems":
			schema.minItems, err = claudeSchemaInt(pointer, key, value)
		case "maxItems":
			schema.maxItems, err = claudeSchemaInt(pointer, key, value)
		case "uniqueItems":
			flag, ok := value.(bool)
			if !ok {
				err = fmt.Errorf("uniqueItems at %s must be a boolean", pointer)
			}
			schema.uniqueItems = flag
		case "minLength":
			schema.minLength, err = claudeSchemaInt(pointer, key, value)
		case "maxLength":
			schema.maxLength, err = claudeSchemaInt(pointer, key, value)
		case "pattern":
			text, ok := value.(string)
			if !ok {
				err = fmt.Errorf("pattern at %s must be a string", pointer)
				break
			}
			// Go's RE2 rejects the backtracking constructs ECMA-262 allows. A
			// pattern that does not compile here is a pattern this adapter cannot
			// enforce, so it refuses rather than skipping the keyword.
			compiled, compileErr := regexp.Compile(text)
			if compileErr != nil {
				err = fmt.Errorf(
					"pattern %q at %s uses a regular-expression construct this adapter cannot evaluate: %v",
					text, pointer, compileErr,
				)
				break
			}
			schema.pattern = compiled
			schema.patternRaw = text
		case "minimum":
			schema.minimum, err = claudeSchemaRat(pointer, key, value)
		case "maximum":
			schema.maximum, err = claudeSchemaRat(pointer, key, value)
		case "exclusiveMinimum":
			schema.exclusiveMinimum, err = claudeSchemaRat(pointer, key, value)
		case "exclusiveMaximum":
			schema.exclusiveMaximum, err = claudeSchemaRat(pointer, key, value)
		case "multipleOf":
			schema.multipleOf, err = claudeSchemaRat(pointer, key, value)
			if err == nil && schema.multipleOf.Sign() <= 0 {
				err = fmt.Errorf("multipleOf at %s must be greater than zero", pointer)
			}
		case "allOf":
			schema.allOf, err = claudeSchemaNodes(pointer, key, value)
		case "anyOf":
			schema.anyOf, err = claudeSchemaNodes(pointer, key, value)
		case "oneOf":
			schema.oneOf, err = claudeSchemaNodes(pointer, key, value)
		case "not":
			schema.not, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "if":
			schema.ifSchema, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "then":
			schema.thenSchema, err = compileClaudeAssayNode(pointer+"/"+key, value)
		case "else":
			schema.elseSchema, err = compileClaudeAssayNode(pointer+"/"+key, value)
		default:
			if claudeAssayAnnotationKeywords[key] {
				continue
			}
			// $ref, $defs, patternProperties, prefixItems, contains,
			// unevaluatedProperties and format all land here on purpose. Each one
			// would change what a conformant answer is, and an adapter that skipped
			// them would be claiming an enforcement it did not perform.
			err = fmt.Errorf(
				"schema keyword %q at %s is outside the subset this adapter can enforce", key, pointer,
			)
		}
		if err != nil {
			return nil, err
		}
	}
	return schema, nil
}

// check reports the first way value violates the schema, with a JSON path an
// operator can locate in the provider's answer.
func (s *claudeAssaySchema) check(path string, value any) error {
	if s == nil {
		return nil
	}
	if s.boolean != nil {
		if *s.boolean {
			return nil
		}
		return fmt.Errorf("%s is not allowed here", path)
	}
	if len(s.types) > 0 && !claudeSchemaTypeMatches(s.types, value) {
		return fmt.Errorf(
			"%s is %s where %s was required", path, claudeJSONTypeName(value), strings.Join(s.types, " or "),
		)
	}
	if s.constValue != nil && !claudeJSONValueEqual(*s.constValue, value) {
		return fmt.Errorf("%s is not the single value the schema fixes", path)
	}
	if len(s.enumValues) > 0 {
		matched := false
		for _, candidate := range s.enumValues {
			if claudeJSONValueEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not one of the values the schema enumerates", path)
		}
	}
	if err := s.checkObject(path, value); err != nil {
		return err
	}
	if err := s.checkArray(path, value); err != nil {
		return err
	}
	if err := s.checkString(path, value); err != nil {
		return err
	}
	if err := s.checkNumber(path, value); err != nil {
		return err
	}
	return s.checkCombinators(path, value)
}

func (s *claudeAssaySchema) checkObject(path string, value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if s.minProperties != nil && len(object) < *s.minProperties {
		return fmt.Errorf("%s holds %d member(s) where at least %d were required",
			path, len(object), *s.minProperties)
	}
	if s.maxProperties != nil && len(object) > *s.maxProperties {
		return fmt.Errorf("%s holds %d member(s) where at most %d were allowed",
			path, len(object), *s.maxProperties)
	}
	for _, name := range s.required {
		if _, present := object[name]; !present {
			return fmt.Errorf("%s is missing required member %q", path, name)
		}
	}
	for _, name := range claudeSortedKeys(s.dependentRequired) {
		if _, present := object[name]; !present {
			continue
		}
		for _, dependent := range s.dependentRequired[name] {
			if _, ok := object[dependent]; !ok {
				return fmt.Errorf("%s carries %q without the %q it requires", path, name, dependent)
			}
		}
	}
	for _, name := range claudeSortedKeys(object) {
		member := object[name]
		if s.propertyNames != nil {
			if err := s.propertyNames.check(path+"."+name, name); err != nil {
				return fmt.Errorf("member name %q at %s is not permitted: %w", name, path, err)
			}
		}
		if declared, ok := s.properties[name]; ok {
			if err := declared.check(path+"."+name, member); err != nil {
				return err
			}
			continue
		}
		if s.additional == nil {
			continue
		}
		if s.additional.boolean != nil && !*s.additional.boolean {
			return fmt.Errorf("%s carries member %q the schema does not declare", path, name)
		}
		if err := s.additional.check(path+"."+name, member); err != nil {
			return err
		}
	}
	return nil
}

func (s *claudeAssaySchema) checkArray(path string, value any) error {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	if s.minItems != nil && len(items) < *s.minItems {
		return fmt.Errorf("%s holds %d item(s) where at least %d were required",
			path, len(items), *s.minItems)
	}
	if s.maxItems != nil && len(items) > *s.maxItems {
		return fmt.Errorf("%s holds %d item(s) where at most %d were allowed",
			path, len(items), *s.maxItems)
	}
	if s.uniqueItems {
		for outer := range items {
			for inner := outer + 1; inner < len(items); inner++ {
				if claudeJSONValueEqual(items[outer], items[inner]) {
					return fmt.Errorf("%s repeats the item at index %d", path, outer)
				}
			}
		}
	}
	if s.items == nil {
		return nil
	}
	for index, item := range items {
		if err := s.items.check(fmt.Sprintf("%s[%d]", path, index), item); err != nil {
			return err
		}
	}
	return nil
}

func (s *claudeAssaySchema) checkString(path string, value any) error {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	length := len([]rune(text))
	if s.minLength != nil && length < *s.minLength {
		return fmt.Errorf("%s is %d character(s) where at least %d were required",
			path, length, *s.minLength)
	}
	if s.maxLength != nil && length > *s.maxLength {
		return fmt.Errorf("%s is %d character(s) where at most %d were allowed",
			path, length, *s.maxLength)
	}
	if s.pattern != nil && !s.pattern.MatchString(text) {
		return fmt.Errorf("%s does not match the pattern %q the schema requires", path, s.patternRaw)
	}
	return nil
}

// checkNumber evaluates every numeric assertion in exact rational arithmetic.
//
// The document's literal is converted to a rational once and compared exactly, so
// a bound is not satisfied by rounding and divisibility is decided arithmetically
// rather than by a tolerance. The previous revision compared through float64 and a
// 1e-9 quotient tolerance, which accepted 9007199254740993 against
// {"maximum":9007199254740992} and made decimal multipleOf a near-miss test.
func (s *claudeAssaySchema) checkNumber(path string, value any) error {
	number, ok := value.(json.Number)
	if !ok {
		return nil
	}
	if s.minimum == nil && s.maximum == nil &&
		s.exclusiveMinimum == nil && s.exclusiveMaximum == nil && s.multipleOf == nil {
		return nil
	}
	parsed, err := claudeExactNumber(number)
	if err != nil {
		return fmt.Errorf("%s is not a number this adapter can evaluate exactly: %v", path, err)
	}
	if s.minimum != nil && parsed.Cmp(s.minimum) < 0 {
		return fmt.Errorf("%s is below the minimum the schema sets", path)
	}
	if s.maximum != nil && parsed.Cmp(s.maximum) > 0 {
		return fmt.Errorf("%s is above the maximum the schema sets", path)
	}
	if s.exclusiveMinimum != nil && parsed.Cmp(s.exclusiveMinimum) <= 0 {
		return fmt.Errorf("%s is not above the exclusive minimum the schema sets", path)
	}
	if s.exclusiveMaximum != nil && parsed.Cmp(s.exclusiveMaximum) >= 0 {
		return fmt.Errorf("%s is not below the exclusive maximum the schema sets", path)
	}
	if s.multipleOf != nil {
		// Exact divisibility: the quotient of two rationals is a rational, and it is
		// an integer precisely when the division leaves no remainder.
		quotient := new(big.Rat).Quo(parsed, s.multipleOf)
		if !quotient.IsInt() {
			return fmt.Errorf("%s is not a multiple of the value the schema requires", path)
		}
	}
	return nil
}

func (s *claudeAssaySchema) checkCombinators(path string, value any) error {
	for index, branch := range s.allOf {
		if err := branch.check(path, value); err != nil {
			return fmt.Errorf("%s fails allOf branch %d: %w", path, index, err)
		}
	}
	if len(s.anyOf) > 0 {
		matched := false
		for _, branch := range s.anyOf {
			if branch.check(path, value) == nil {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s satisfies none of the anyOf branches", path)
		}
	}
	if len(s.oneOf) > 0 {
		matches := 0
		for _, branch := range s.oneOf {
			if branch.check(path, value) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf(
				"%s satisfies %d oneOf branches where exactly one was required", path, matches,
			)
		}
	}
	if s.not != nil && s.not.check(path, value) == nil {
		return fmt.Errorf("%s matches a shape the schema forbids", path)
	}
	if s.ifSchema != nil {
		if s.ifSchema.check(path, value) == nil {
			if err := s.thenSchema.check(path, value); err != nil {
				return err
			}
		} else if err := s.elseSchema.check(path, value); err != nil {
			return err
		}
	}
	return nil
}

// claudeAssaySchemaDigest binds one exact schema to the attestation.
//
// It is taken over the decoded document rather than the caller's bytes, so
// whitespace and member order cannot make two identical schemas look different
// while a real change always does.
func claudeAssaySchemaDigest(raw json.RawMessage) (string, error) {
	decoded, err := claudeDecodeJSONValue(raw)
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// claudeDecodeJSONValue decodes a whole JSON document, keeping numbers in their
// original literal form so an integer keyword is not decided by float64 rounding.
func claudeDecodeJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, fmt.Errorf("trailing JSON after the document")
	}
	return value, nil
}

func claudeJSONTypeName(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number:
		if claudeNumberIsIntegral(typed) {
			return "integer"
		}
		return "number"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	}
	return "an unrecognized JSON value"
}

func claudeSchemaTypeMatches(types []string, value any) bool {
	actual := claudeJSONTypeName(value)
	for _, want := range types {
		if want == actual {
			return true
		}
		// Every integer is also a number; the reverse is not true.
		if want == "number" && actual == "integer" {
			return true
		}
	}
	return false
}

// claudeAssayNumberLiteralLimit and claudeAssayNumberExponentLimit bound what
// this adapter will convert to an exact rational.
//
// Exactness has a cost that a schema or an answer controls: 1e1000000000 is a
// three-character exponent describing a number with a billion digits, and
// materializing it would trade a rounding bug for a memory one. The bounds are far
// outside anything a fact-result document uses — binary64 itself tops out near
// 1e308 — and a literal beyond them is refused rather than approximated, so the
// governing rule still holds: every accepted assertion is enforced exactly.
const (
	claudeAssayNumberLiteralLimit  = 512
	claudeAssayNumberExponentLimit = 1024
)

// claudeExactNumber converts one JSON numeric literal to an exact rational.
//
// big.Rat parses the whole JSON number grammar — integer, fraction and exponent —
// without loss, so the decimal the caller wrote is the value that gets compared.
// The bounds are checked first because they are what keeps that exactness cheap.
func claudeExactNumber(number json.Number) (*big.Rat, error) {
	literal := number.String()
	if len(literal) > claudeAssayNumberLiteralLimit {
		return nil, fmt.Errorf(
			"the numeric literal is %d characters, above the %d-character limit for exact comparison",
			len(literal), claudeAssayNumberLiteralLimit,
		)
	}
	if exponent, ok := claudeNumberExponent(literal); ok {
		if exponent > claudeAssayNumberExponentLimit || exponent < -claudeAssayNumberExponentLimit {
			return nil, fmt.Errorf(
				"the numeric literal carries exponent %d, outside the ±%d range this adapter compares exactly",
				exponent, claudeAssayNumberExponentLimit,
			)
		}
	}
	parsed, ok := new(big.Rat).SetString(literal)
	if !ok {
		return nil, fmt.Errorf("%q is not a numeric literal this adapter can read", literal)
	}
	return parsed, nil
}

// claudeNumberExponent reads the explicit decimal exponent of a JSON literal.
//
// A literal with no exponent part reports false, which is not a failure: its
// magnitude is already bounded by the literal-length limit.
func claudeNumberExponent(literal string) (int, bool) {
	index := strings.IndexAny(literal, "eE")
	if index < 0 {
		return 0, false
	}
	exponent, err := strconv.Atoi(literal[index+1:])
	if err != nil {
		// Unreadable here means either malformed — which the JSON decoder already
		// rejected — or wider than an int, which is far outside the limit either way.
		return 0, false
	}
	return exponent, true
}

// claudeRequireExactNumbers refuses a document holding a numeric literal this
// adapter cannot compare exactly.
//
// It runs over the caller's schema at compile time and over the provider's answer
// before enforcement, which is what lets every comparison below treat exact
// conversion as guaranteed instead of falling back to an approximation on the one
// input where precision mattered. Refusing up front also names the real problem in
// the right place: an unrepresentable literal in the schema is a capability
// failure, and one in the answer is an answer this adapter will not vouch for.
func claudeRequireExactNumbers(pointer string, value any) error {
	switch typed := value.(type) {
	case json.Number:
		if _, err := claudeExactNumber(typed); err != nil {
			return fmt.Errorf("the number at %s cannot be compared exactly: %w", pointer, err)
		}
	case map[string]any:
		for _, key := range claudeSortedKeys(typed) {
			if err := claudeRequireExactNumbers(pointer+"/"+key, typed[key]); err != nil {
				return err
			}
		}
	case []any:
		for index, member := range typed {
			if err := claudeRequireExactNumbers(fmt.Sprintf("%s/%d", pointer, index), member); err != nil {
				return err
			}
		}
	}
	return nil
}

// claudeNumberIsIntegral decides the integer type keyword exactly.
//
// JSON Schema calls a number an integer when it has no fractional part, which is a
// property of the value rather than of how it was spelled: 1.0 and 1e2 are
// integers. Deciding it through float64 got both ends wrong — a literal too large
// for binary64 was reported as non-integral, and a non-integral literal beyond
// binary64's precision was rounded into looking integral.
func claudeNumberIsIntegral(number json.Number) bool {
	if _, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
		return true
	}
	parsed, err := claudeExactNumber(number)
	if err != nil {
		return false
	}
	return parsed.IsInt()
}

// claudeJSONValueEqual is JSON equality, with numbers compared as exact decimals
// so 1 and 1.0 are the same enum member while 9007199254740992 and
// 9007199254740993 are not.
func claudeJSONValueEqual(left, right any) bool {
	switch typedLeft := left.(type) {
	case nil:
		return right == nil
	case bool:
		typedRight, ok := right.(bool)
		return ok && typedLeft == typedRight
	case string:
		typedRight, ok := right.(string)
		return ok && typedLeft == typedRight
	case json.Number:
		typedRight, ok := right.(json.Number)
		if !ok {
			return false
		}
		if typedLeft.String() == typedRight.String() {
			return true
		}
		// Differing literals are compared as exact rationals, never through
		// binary64: rounding two distinct decimals onto the same float is precisely
		// how a const or enum assertion accepts a value it forbids. An
		// unrepresentable literal cannot reach here — both the schema and the answer
		// are screened by claudeRequireExactNumbers — and if one ever did, unequal is
		// the fail-closed answer for const and enum.
		leftValue, leftErr := claudeExactNumber(typedLeft)
		rightValue, rightErr := claudeExactNumber(typedRight)
		return leftErr == nil && rightErr == nil && leftValue.Cmp(rightValue) == 0
	case map[string]any:
		typedRight, ok := right.(map[string]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for key, member := range typedLeft {
			other, present := typedRight[key]
			if !present || !claudeJSONValueEqual(member, other) {
				return false
			}
		}
		return true
	case []any:
		typedRight, ok := right.([]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for index := range typedLeft {
			if !claudeJSONValueEqual(typedLeft[index], typedRight[index]) {
				return false
			}
		}
		return true
	}
	return false
}

// --- keyword decoding -----------------------------------------------------

func claudeSchemaTypeNames(pointer string, node any) ([]string, error) {
	known := map[string]bool{
		"object": true, "array": true, "string": true,
		"number": true, "integer": true, "boolean": true, "null": true,
	}
	collect := func(candidate any) (string, error) {
		name, ok := candidate.(string)
		if !ok {
			return "", fmt.Errorf("type at %s must be a string or an array of strings", pointer)
		}
		if !known[name] {
			return "", fmt.Errorf("type %q at %s is not a JSON type", name, pointer)
		}
		return name, nil
	}
	if list, ok := node.([]any); ok {
		if len(list) == 0 {
			return nil, fmt.Errorf("type at %s is empty, so it can never be satisfied", pointer)
		}
		names := make([]string, 0, len(list))
		for _, candidate := range list {
			name, err := collect(candidate)
			if err != nil {
				return nil, err
			}
			names = append(names, name)
		}
		return names, nil
	}
	name, err := collect(node)
	if err != nil {
		return nil, err
	}
	return []string{name}, nil
}

func claudeSchemaMap(pointer, keyword string, node any) (map[string]*claudeAssaySchema, error) {
	object, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be an object", keyword, pointer)
	}
	compiled := make(map[string]*claudeAssaySchema, len(object))
	for _, name := range claudeSortedKeys(object) {
		member, err := compileClaudeAssayNode(pointer+"/"+keyword+"/"+name, object[name])
		if err != nil {
			return nil, err
		}
		compiled[name] = member
	}
	return compiled, nil
}

func claudeSchemaNodes(pointer, keyword string, node any) ([]*claudeAssaySchema, error) {
	list, ok := node.([]any)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be an array of schemas", keyword, pointer)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("%s at %s is empty, so it can never be satisfied", keyword, pointer)
	}
	compiled := make([]*claudeAssaySchema, 0, len(list))
	for index, member := range list {
		branch, err := compileClaudeAssayNode(fmt.Sprintf("%s/%s/%d", pointer, keyword, index), member)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, branch)
	}
	return compiled, nil
}

func claudeSchemaArray(pointer, keyword string, node any) ([]any, error) {
	list, ok := node.([]any)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be an array", keyword, pointer)
	}
	return list, nil
}

func claudeSchemaStrings(pointer, keyword string, node any) ([]string, error) {
	list, err := claudeSchemaArray(pointer, keyword, node)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list))
	for _, member := range list {
		name, ok := member.(string)
		if !ok {
			return nil, fmt.Errorf("%s at %s must hold only strings", keyword, pointer)
		}
		names = append(names, name)
	}
	return names, nil
}

func claudeSchemaStringLists(pointer, keyword string, node any) (map[string][]string, error) {
	object, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be an object", keyword, pointer)
	}
	lists := make(map[string][]string, len(object))
	for _, name := range claudeSortedKeys(object) {
		values, err := claudeSchemaStrings(pointer+"/"+keyword, name, object[name])
		if err != nil {
			return nil, err
		}
		lists[name] = values
	}
	return lists, nil
}

func claudeSchemaInt(pointer, keyword string, node any) (*int, error) {
	number, ok := node.(json.Number)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be a number", keyword, pointer)
	}
	parsed, err := strconv.Atoi(number.String())
	if err != nil {
		return nil, fmt.Errorf("%s at %s must be an integer", keyword, pointer)
	}
	if parsed < 0 {
		return nil, fmt.Errorf("%s at %s must not be negative", keyword, pointer)
	}
	return &parsed, nil
}

// claudeSchemaRat decodes one numeric keyword as an exact rational, so the bound
// the caller wrote is the bound that is enforced.
func claudeSchemaRat(pointer, keyword string, node any) (*big.Rat, error) {
	number, ok := node.(json.Number)
	if !ok {
		return nil, fmt.Errorf("%s at %s must be a number", keyword, pointer)
	}
	parsed, err := claudeExactNumber(number)
	if err != nil {
		return nil, fmt.Errorf(
			"%s at %s is a number this adapter cannot enforce exactly: %v", keyword, pointer, err)
	}
	return parsed, nil
}

func claudeSortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
