package cosmoguard

import (
	"fmt"
	"os"
	"strings"
)

// EnvInterpolate expands shell-style variable references in s. Supported forms:
//
//	${VAR}              — replaced with os.Getenv("VAR"); error if unset OR empty
//	${VAR:-default}     — replaced with os.Getenv("VAR"), or "default" if unset OR empty
//	${VAR:?message}     — replaced with os.Getenv("VAR"); error with "message" if unset OR empty
//
// $VAR (without braces) is intentionally NOT expanded — bare names collide too
// easily with YAML content (e.g. `$ref`, `$id`) and the brace form is clearer.
//
// EMPTY-STRING SEMANTICS: Unlike strict POSIX shell, all three forms treat
// `VAR=""` (set but empty) the same as `VAR` unset. This is the right default
// for config secrets: an empty token is always a configuration mistake, not a
// deliberate "disable" signal. If you genuinely need to pass an empty value,
// use the `:-` default with an empty default (`${VAR:-}`).
//
// The default form preserves any `:` inside the default value: ${URL:-http://x:1}
// expands to "http://x:1" when URL is unset.
//
// Returns the expanded string and any error from missing variables. The error
// message identifies the variable name (and the user-supplied message if the
// `:?` form was used) so config-load failures are diagnostic.
func EnvInterpolate(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))

	inSingleQuoted := false
	inDoubleQuoted := false
	for i := 0; i < len(s); {
		c := s[i]
		// Track YAML string-quoting state so a `#` inside a quoted scalar
		// isn't mistaken for a comment.
		if c == '\'' && !inDoubleQuoted {
			inSingleQuoted = !inSingleQuoted
			b.WriteByte(c)
			i++
			continue
		}
		if c == '"' && !inSingleQuoted {
			inDoubleQuoted = !inDoubleQuoted
			b.WriteByte(c)
			i++
			continue
		}
		// YAML comment: per YAML 1.2 §6.6, `#` is only a comment
		// indicator when it appears at the start of a line OR is
		// preceded by whitespace. Without the preceding-whitespace
		// check we'd mis-classify `${VAR}` references inside URL
		// fragments (e.g. `http://host#${PATH}`) as comments and
		// silently skip the substitution. inSingleQuoted /
		// inDoubleQuoted gates apply on top so quoted scalars
		// still pass through.
		if c == '#' && !inSingleQuoted && !inDoubleQuoted && isYAMLCommentStart(s, i) {
			eol := strings.IndexByte(s[i:], '\n')
			if eol < 0 {
				b.WriteString(s[i:])
				return b.String(), nil
			}
			b.WriteString(s[i : i+eol+1])
			i += eol + 1
			continue
		}
		// Look for the next ${
		if s[i] != '$' || i+1 >= len(s) || s[i+1] != '{' {
			b.WriteByte(s[i])
			i++
			continue
		}
		// Find matching '}' — we don't support nesting; the first '}' wins.
		end := strings.IndexByte(s[i+2:], '}')
		if end < 0 {
			// Unmatched ${ — preserve verbatim so YAML parse errors point
			// at the original text, not our partial rewrite.
			b.WriteByte(s[i])
			i++
			continue
		}
		ref := s[i+2 : i+2+end]
		i = i + 2 + end + 1

		val, err := resolveVar(ref)
		if err != nil {
			return "", err
		}
		b.WriteString(val)
	}
	return b.String(), nil
}

func resolveVar(ref string) (string, error) {
	// Forms:
	//   NAME            → require set
	//   NAME:-default   → default if unset
	//   NAME:?message   → error with message if unset
	name := ref
	var op, arg string
	if sep := strings.Index(ref, ":-"); sep >= 0 {
		name, op, arg = ref[:sep], ":-", ref[sep+2:]
	} else if sep := strings.Index(ref, ":?"); sep >= 0 {
		name, op, arg = ref[:sep], ":?", ref[sep+2:]
	}
	if name == "" {
		return "", fmt.Errorf("env interpolation: empty variable name in ${%s}", ref)
	}

	v, set := os.LookupEnv(name)
	if set && v != "" {
		return v, nil
	}
	switch op {
	case ":-":
		return arg, nil
	case ":?":
		return "", fmt.Errorf("env interpolation: %s: %s", name, arg)
	default:
		return "", fmt.Errorf("env interpolation: required variable %q is not set", name)
	}
}

// isYAMLCommentStart reports whether the `#` at s[i] is acting as a
// YAML comment indicator per YAML 1.2 §6.6: at the very start of the
// string, the start of a line (preceded by `\n`), or preceded by
// whitespace. Anything else (e.g. `http://host#frag`) is a regular
// character inside a plain scalar.
func isYAMLCommentStart(s string, i int) bool {
	if i == 0 {
		return true
	}
	prev := s[i-1]
	return prev == ' ' || prev == '\t' || prev == '\n' || prev == '\r'
}
