package sessionmanager

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestClaudeAssaySchemaEnforcesTheCV2FactResultUnion is the validator's primary
// case: the exact schema CV-2 describes, checked against the shapes CV-2 calls
// conformant and the shapes it calls malformed.
//
// Every rejected case below is a valid, non-empty JSON object, which is what the
// previous revision's only check accepted.
func TestClaudeAssaySchemaEnforcesTheCV2FactResultUnion(t *testing.T) {
	schema, err := compileClaudeAssaySchema(claudeCV2Schema("f1", "f2"))
	if err != nil {
		t.Fatalf("the CV-2 answer schema is not enforceable: %v", err)
	}
	for _, answer := range []string{
		`{"f1":{"outcome":"answer","value":"40"},"f2":{"outcome":"abstain"}}`,
		`{"f1":{"outcome":"abstain"},"f2":{"outcome":"answer","value":""}}`,
		`{"f2":{"outcome":"abstain"},"f1":{"outcome":"abstain"}}`,
	} {
		if err := claudeCheckAnswer(t, schema, answer); err != nil {
			t.Fatalf("a conformant answer was rejected: %s: %v", answer, err)
		}
	}
	for _, test := range []struct {
		name   string
		answer string
	}{
		{name: "missing fact id", answer: `{"f1":{"outcome":"abstain"}}`},
		{
			name: "extra fact id",
			answer: `{"f1":{"outcome":"abstain"},"f2":{"outcome":"abstain"},` +
				`"f3":{"outcome":"abstain"}}`,
		},
		{name: "unknown outcome", answer: `{"f1":{"outcome":"maybe"},"f2":{"outcome":"abstain"}}`},
		{name: "answer without value", answer: `{"f1":{"outcome":"answer"},"f2":{"outcome":"abstain"}}`},
		{
			name:   "abstain with value",
			answer: `{"f1":{"outcome":"abstain","value":"40"},"f2":{"outcome":"abstain"}}`,
		},
		{
			name: "extra member in a fact result",
			answer: `{"f1":{"outcome":"answer","value":"40","note":"x"},` +
				`"f2":{"outcome":"abstain"}}`,
		},
		{
			name:   "value of the wrong type",
			answer: `{"f1":{"outcome":"answer","value":40},"f2":{"outcome":"abstain"}}`,
		},
		{name: "fact result is not an object", answer: `{"f1":"40","f2":"abstain"}`},
		{name: "generic non-empty object", answer: `{"anything":1}`},
		{name: "empty object", answer: `{}`},
		{name: "an array", answer: `[{"outcome":"abstain"}]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := claudeCheckAnswer(t, schema, test.answer); err == nil {
				t.Fatalf("a malformed answer conformed: %s", test.answer)
			}
		})
	}
}

// TestClaudeAssaySchemaViolationNamesWhereItFailed keeps the refusal actionable:
// a validator that only says "invalid" makes a red assay unlocatable.
func TestClaudeAssaySchemaViolationNamesWhereItFailed(t *testing.T) {
	schema, err := compileClaudeAssaySchema(json.RawMessage(
		`{"type":"object","properties":{"f1":{"type":"object",` +
			`"properties":{"outcome":{"type":"string"}},"required":["outcome"]}},"required":["f1"]}`))
	if err != nil {
		t.Fatal(err)
	}
	err = claudeCheckAnswer(t, schema, `{"f1":{"outcome":7}}`)
	if err == nil {
		t.Fatal("a wrongly typed member conformed")
	}
	if !strings.Contains(err.Error(), "$.f1.outcome") {
		t.Fatalf("the violation does not name the member that failed: %v", err)
	}
}

// TestClaudeAssaySchemaEnforcesEveryKeywordItAccepts walks the subset one keyword
// at a time. The governing rule is that an accepted keyword is an enforced
// keyword, so each row here is one accepting and one rejecting document.
func TestClaudeAssaySchemaEnforcesEveryKeywordItAccepts(t *testing.T) {
	for _, test := range []struct {
		name   string
		schema string
		ok     string
		bad    string
	}{
		{name: "type", schema: `{"type":"string"}`, ok: `"x"`, bad: `1`},
		{name: "type list", schema: `{"type":["string","null"]}`, ok: `null`, bad: `1`},
		{name: "integer", schema: `{"type":"integer"}`, ok: `4`, bad: `4.5`},
		{name: "number accepts integer", schema: `{"type":"number"}`, ok: `4`, bad: `"4"`},
		{name: "enum", schema: `{"enum":["a","b"]}`, ok: `"b"`, bad: `"c"`},
		{name: "const", schema: `{"const":"answer"}`, ok: `"answer"`, bad: `"abstain"`},
		{name: "const numeric equality", schema: `{"const":1}`, ok: `1.0`, bad: `2`},
		{
			name:   "required",
			schema: `{"type":"object","required":["a"]}`,
			ok:     `{"a":1}`,
			bad:    `{"b":1}`,
		},
		{
			name:   "additionalProperties false",
			schema: `{"type":"object","properties":{"a":{}},"additionalProperties":false}`,
			ok:     `{"a":1}`,
			bad:    `{"a":1,"b":2}`,
		},
		{
			name:   "additionalProperties schema",
			schema: `{"type":"object","additionalProperties":{"type":"string"}}`,
			ok:     `{"a":"x"}`,
			bad:    `{"a":1}`,
		},
		{
			name:   "propertyNames",
			schema: `{"type":"object","propertyNames":{"pattern":"^f[0-9]+$"}}`,
			ok:     `{"f1":1}`,
			bad:    `{"fact":1}`,
		},
		{
			name:   "minProperties",
			schema: `{"type":"object","minProperties":2}`,
			ok:     `{"a":1,"b":2}`,
			bad:    `{"a":1}`,
		},
		{
			name:   "maxProperties",
			schema: `{"type":"object","maxProperties":1}`,
			ok:     `{"a":1}`,
			bad:    `{"a":1,"b":2}`,
		},
		{
			name:   "dependentRequired",
			schema: `{"type":"object","dependentRequired":{"value":["outcome"]}}`,
			ok:     `{"value":1,"outcome":"answer"}`,
			bad:    `{"value":1}`,
		},
		{
			name:   "items",
			schema: `{"type":"array","items":{"type":"string"}}`,
			ok:     `["a"]`,
			bad:    `["a",2]`,
		},
		{name: "minItems", schema: `{"type":"array","minItems":1}`, ok: `[1]`, bad: `[]`},
		{name: "maxItems", schema: `{"type":"array","maxItems":1}`, ok: `[1]`, bad: `[1,2]`},
		{
			name:   "uniqueItems",
			schema: `{"type":"array","uniqueItems":true}`,
			ok:     `[1,2]`,
			bad:    `[1,1]`,
		},
		{name: "minLength", schema: `{"type":"string","minLength":2}`, ok: `"ab"`, bad: `"a"`},
		{name: "maxLength", schema: `{"type":"string","maxLength":1}`, ok: `"a"`, bad: `"ab"`},
		{name: "pattern", schema: `{"type":"string","pattern":"^f"}`, ok: `"f1"`, bad: `"x1"`},
		{name: "minimum", schema: `{"minimum":2}`, ok: `2`, bad: `1`},
		{name: "maximum", schema: `{"maximum":2}`, ok: `2`, bad: `3`},
		{name: "exclusiveMinimum", schema: `{"exclusiveMinimum":2}`, ok: `3`, bad: `2`},
		{name: "exclusiveMaximum", schema: `{"exclusiveMaximum":2}`, ok: `1`, bad: `2`},
		{name: "multipleOf", schema: `{"multipleOf":5}`, ok: `10`, bad: `7`},
		{
			name:   "allOf",
			schema: `{"allOf":[{"type":"string"},{"minLength":2}]}`,
			ok:     `"ab"`,
			bad:    `"a"`,
		},
		{
			name:   "anyOf",
			schema: `{"anyOf":[{"type":"string"},{"type":"integer"}]}`,
			ok:     `4`,
			bad:    `true`,
		},
		{
			name:   "oneOf rejects a value matching both branches",
			schema: `{"oneOf":[{"type":"integer"},{"minimum":0}]}`,
			ok:     `-1`,
			bad:    `4`,
		},
		{name: "not", schema: `{"not":{"type":"string"}}`, ok: `1`, bad: `"a"`},
		{
			name: "if then else",
			schema: `{"if":{"properties":{"outcome":{"const":"answer"}},"required":["outcome"]},` +
				`"then":{"required":["value"]},"else":{"maxProperties":1}}`,
			ok:  `{"outcome":"answer","value":"x"}`,
			bad: `{"outcome":"answer"}`,
		},
		{
			name:   "if else branch",
			schema: `{"if":{"const":"answer"},"then":{"maxLength":99},"else":{"maxLength":1}}`,
			ok:     `"a"`,
			bad:    `"abstain"`,
		},
		{name: "boolean false schema", schema: `false`, ok: ``, bad: `1`},
		{name: "boolean true schema", schema: `true`, ok: `1`, bad: ``},
		{
			name:   "annotations constrain nothing",
			schema: `{"title":"t","description":"d","default":1,"examples":[1],"type":"integer"}`,
			ok:     `1`,
			bad:    `"1"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema, err := compileClaudeAssaySchema(json.RawMessage(test.schema))
			if err != nil {
				t.Fatalf("schema %s is not enforceable: %v", test.schema, err)
			}
			if test.ok != "" {
				if err := claudeCheckAnswer(t, schema, test.ok); err != nil {
					t.Fatalf("conformant document %s was rejected: %v", test.ok, err)
				}
			}
			if test.bad != "" {
				if err := claudeCheckAnswer(t, schema, test.bad); err == nil {
					t.Fatalf("non-conformant document %s was accepted", test.bad)
				}
			}
		})
	}
}

// TestClaudeAssaySchemaRefusesConstructsItCannotEnforce is the fail-closed rule.
//
// This adapter has no provider-side fallback: Claude implements
// schema-constrained output as a tool and the assay denies every tool. So an
// unenforceable keyword cannot be delegated and must not be skipped either —
// skipping it would mean the attestation claims an enforcement that did not
// happen. It is refused at compile time, before a provider turn is spent.
func TestClaudeAssaySchemaRefusesConstructsItCannotEnforce(t *testing.T) {
	for _, test := range []struct {
		name    string
		schema  string
		mention string
	}{
		{name: "$ref", schema: `{"$ref":"#/$defs/fact"}`, mention: "$ref"},
		{name: "$defs", schema: `{"$defs":{"fact":{}}}`, mention: "$defs"},
		{name: "definitions", schema: `{"definitions":{"fact":{}}}`, mention: "definitions"},
		{
			name:    "patternProperties",
			schema:  `{"patternProperties":{"^f":{}}}`,
			mention: "patternProperties",
		},
		{name: "prefixItems", schema: `{"prefixItems":[{}]}`, mention: "prefixItems"},
		{name: "contains", schema: `{"contains":{}}`, mention: "contains"},
		{
			name:    "unevaluatedProperties",
			schema:  `{"unevaluatedProperties":false}`,
			mention: "unevaluatedProperties",
		},
		{name: "format", schema: `{"format":"uuid"}`, mention: "format"},
		{name: "nested unsupported keyword", schema: `{"properties":{"f1":{"format":"uuid"}}}`,
			mention: "format"},
		{name: "an uncompilable pattern", schema: `{"pattern":"(?=x)"}`, mention: "pattern"},
		{name: "a non-schema document", schema: `[1]`, mention: "must be an object or a boolean"},
		{name: "an unknown type name", schema: `{"type":"fact"}`, mention: "is not a JSON type"},
		{name: "a non-string type", schema: `{"type":7}`, mention: "must be a string"},
		{name: "an empty enum", schema: `{"enum":[]}`, mention: "empty"},
		{name: "an empty oneOf", schema: `{"oneOf":[]}`, mention: "empty"},
		{name: "a negative maxItems", schema: `{"maxItems":-1}`, mention: "negative"},
		{name: "a non-integer minItems", schema: `{"minItems":1.5}`, mention: "integer"},
		{name: "a zero multipleOf", schema: `{"multipleOf":0}`, mention: "greater than zero"},
		{name: "unparsable JSON", schema: `{`, mention: "not valid JSON"},
		{name: "trailing JSON", schema: `{} {}`, mention: "trailing JSON"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := compileClaudeAssaySchema(json.RawMessage(test.schema))
			if err == nil {
				t.Fatalf("schema %s was accepted as enforceable", test.schema)
			}
			if !strings.Contains(err.Error(), test.mention) {
				t.Fatalf("the refusal does not name what it could not enforce: %v", err)
			}
		})
	}
}

// TestClaudeAssaySchemaDigestIdentifiesTheEnforcedSchema pins what the
// attestation binds. Formatting must not change the digest, because two spellings
// of one schema are one enforcement; a real change must, because that is the whole
// point of recording it.
func TestClaudeAssaySchemaDigestIdentifiesTheEnforcedSchema(t *testing.T) {
	digest, err := claudeAssaySchemaDigest(json.RawMessage(
		`{"type":"object","required":["f1"],"properties":{"f1":{"type":"string"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(digest) != 64 {
		t.Fatalf("schema digest %q is not a sha256 hex digest", digest)
	}
	reordered, err := claudeAssaySchemaDigest(json.RawMessage(
		"{\n \"properties\": {\"f1\": {\"type\": \"string\"}},\n" +
			" \"required\": [\"f1\"],\n \"type\": \"object\"\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if reordered != digest {
		t.Fatal("whitespace and member order changed the enforced-schema digest")
	}
	changed, err := claudeAssaySchemaDigest(json.RawMessage(
		`{"type":"object","required":["f1","f2"],"properties":{"f1":{"type":"string"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed == digest {
		t.Fatal("a schema requiring a different fact set produced the same digest")
	}
}

// TestClaudeAssayRequiresAnAnswerSchema keeps the Codex sibling's bar.
//
// An optional schema left a second, weaker enforcement level reachable, and that
// level — "the answer is a non-empty JSON object" — is exactly the check that let
// malformed output through. There is no provider-side fallback to fall back to
// here, so an absent schema is refused rather than downgraded, and a nil compiled
// schema can never reach the answer gate as a pass.
func TestClaudeAssayRequiresAnAnswerSchema(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(""), json.RawMessage("  ")} {
		if _, _, err := claudeAssayAnswerSchema(raw); err == nil {
			t.Fatalf("an assay with no output schema was accepted: %q", raw)
		}
	}
	if err := requireClaudeAssaySchemaConformance(
		"CTX-260730-a00001", nil, json.RawMessage(`{"anything":1}`),
	); err == nil {
		t.Fatal("an answer reached the scorer with no compiled schema behind it")
	}

	schema, digest, err := claudeAssayAnswerSchema(claudeCV2Schema("f1"))
	if err != nil {
		t.Fatal(err)
	}
	if schema == nil || digest == "" {
		t.Fatal("a supplied schema produced no enforcement state")
	}
}

// TestClaudeAssaySchemaComparesNumbersExactly is the regression for the second
// blocking finding of review cycle 3.
//
// Every numeric assertion used to collapse the document and the schema through
// float64, so distinct JSON numbers compared equal whenever they rounded to the
// same binary64 value, and multipleOf was a 1e-9 near-miss test rather than a
// divisibility test. Both directions were wrong: conformant documents were
// rejected and non-conformant ones accepted, while the attestation still said
// adapter-json-schema.
//
// The literals below sit on the binary64 precision boundary. 9007199254740992 is
// 2^53, the point where consecutive integers stop being representable, so
// 9007199254740993 rounds onto it — which is exactly the case the review cited.
func TestClaudeAssaySchemaComparesNumbersExactly(t *testing.T) {
	for _, test := range []struct {
		name   string
		schema string
		ok     string
		bad    string
	}{
		{
			name:   "const at the binary64 boundary",
			schema: `{"const":9007199254740992}`,
			ok:     `9007199254740992`,
			bad:    `9007199254740993`,
		},
		{
			name:   "enum at the binary64 boundary",
			schema: `{"enum":[9007199254740992,1]}`,
			ok:     `9007199254740992`,
			bad:    `9007199254740993`,
		},
		{
			name:   "maximum at the binary64 boundary",
			schema: `{"maximum":9007199254740992}`,
			ok:     `9007199254740992`,
			bad:    `9007199254740993`,
		},
		{
			name:   "minimum at the binary64 boundary",
			schema: `{"minimum":9007199254740993}`,
			ok:     `9007199254740993`,
			bad:    `9007199254740992`,
		},
		{
			name:   "exclusiveMaximum at the binary64 boundary",
			schema: `{"exclusiveMaximum":9007199254740993}`,
			ok:     `9007199254740992`,
			bad:    `9007199254740993`,
		},
		{
			name:   "exclusiveMinimum at the binary64 boundary",
			schema: `{"exclusiveMinimum":9007199254740992}`,
			ok:     `9007199254740993`,
			bad:    `9007199254740992`,
		},
		{
			// Exact: 9007199254740993 = 3 x 3002399751580331. Through float64 the
			// document rounds down to 2^53 and the quotient misses an integer by a
			// third, so the tolerance test rejected a conformant value.
			name:   "multipleOf divides a value beyond binary64 precision",
			schema: `{"multipleOf":3}`,
			ok:     `9007199254740993`,
			bad:    `9007199254740992`,
		},
		{
			// The tolerance test accepted this: the quotient missed an integer by
			// 1e-10, well inside the 1e-9 slack it allowed.
			name:   "multipleOf is divisibility, not proximity",
			schema: `{"multipleOf":1}`,
			ok:     `2`,
			bad:    `1.0000000001`,
		},
		{
			name:   "decimal multipleOf divides exactly",
			schema: `{"multipleOf":0.1}`,
			ok:     `0.3`,
			bad:    `0.35`,
		},
		{
			name:   "decimal multipleOf with a repeating binary expansion",
			schema: `{"multipleOf":0.0001}`,
			ok:     `1.0001`,
			bad:    `1.00015`,
		},
		{
			// Integrality is a property of the value, not of its float64 image:
			// 9007199254740993.5 rounds onto an even integer, which is how a
			// fractional literal passed as an integer.
			name:   "integer beyond binary64 precision",
			schema: `{"type":"integer"}`,
			ok:     `9007199254740993`,
			bad:    `9007199254740993.5`,
		},
		{
			// Too large for binary64 entirely, but still an integer: 1e400 is 1
			// followed by 400 zeroes. Deciding this through Float64 hit its range
			// error and reported a plain integer as a non-integer number. The
			// rejected literal is the mirror case — small enough to underflow
			// binary64, and genuinely not an integer.
			name:   "integer beyond the binary64 range",
			schema: `{"type":"integer"}`,
			ok:     `1e400`,
			bad:    `1e-400`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema, err := compileClaudeAssaySchema(json.RawMessage(test.schema))
			if err != nil {
				t.Fatalf("schema %s is not enforceable: %v", test.schema, err)
			}
			if test.ok != "" {
				if err := claudeCheckAnswer(t, schema, test.ok); err != nil {
					t.Fatalf("conformant document %s was rejected: %v", test.ok, err)
				}
			}
			if test.bad != "" {
				if err := claudeCheckAnswer(t, schema, test.bad); err == nil {
					t.Fatalf("non-conformant document %s was accepted", test.bad)
				}
			}
		})
	}
}

// TestClaudeAssaySchemaRefusesNumbersItCannotCompareExactly keeps the fail-closed
// rule intact where exactness stops being affordable.
//
// A three-character exponent can describe a number with a billion digits, so
// exactness is bounded rather than unbounded. The bound is far outside anything a
// fact-result document uses — binary64 itself stops near 1e308 — and a literal past
// it is refused on both sides rather than silently approximated: in the caller's
// schema it is a capability failure, and in the provider's answer it is an answer
// this adapter will not claim conforms.
func TestClaudeAssaySchemaRefusesNumbersItCannotCompareExactly(t *testing.T) {
	for _, schema := range []string{
		`{"maximum":1e99999}`,
		`{"const":1e-99999}`,
		`{"enum":[1,1e99999]}`,
		`{"properties":{"a":{"multipleOf":1e99999}}}`,
	} {
		if _, err := compileClaudeAssaySchema(json.RawMessage(schema)); err == nil {
			t.Fatalf("schema %s was accepted although its numbers cannot be compared exactly", schema)
		}
	}
	// Answers are screened before enforcement, so no assertion is ever decided by
	// an approximation of the document either.
	for _, document := range []string{
		`{"f1":{"outcome":"answer","value":1e99999}}`,
		`[1e-99999]`,
	} {
		value, err := claudeDecodeJSONValue([]byte(document))
		if err != nil {
			t.Fatal(err)
		}
		if err := claudeRequireExactNumbers("$", value); err == nil {
			t.Fatalf("document %s was accepted although its numbers cannot be compared exactly", document)
		}
	}
	// Everything inside the bound stays enforceable, so the guard is not a
	// restriction on realistic schemas.
	for _, schema := range []string{
		`{"maximum":1e308}`,
		`{"multipleOf":1e-308}`,
		`{"const":-9007199254740993}`,
	} {
		if _, err := compileClaudeAssaySchema(json.RawMessage(schema)); err != nil {
			t.Fatalf("schema %s should be enforceable exactly: %v", schema, err)
		}
	}
}

func claudeCheckAnswer(t *testing.T, schema *claudeAssaySchema, document string) error {
	t.Helper()
	value, err := claudeDecodeJSONValue([]byte(document))
	if err != nil {
		t.Fatalf("the test document %s is not valid JSON: %v", document, err)
	}
	return schema.check("$", value)
}
