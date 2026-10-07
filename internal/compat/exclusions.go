package compat

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

const defaultMethodExclusion = "/eth.evm.v1.Query/Trace*"

var methodExclusionPattern = regexp.MustCompile(`^/[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+/([A-Za-z_][A-Za-z0-9_]*\*?|\*)$`)

// ValidateMethodExclusions rejects malformed paths before any node contact.
func ValidateMethodExclusions(patterns []string) error {
	for _, p := range patterns {
		if !methodExclusionPattern.MatchString("/" + strings.TrimPrefix(p, "/")) {
			return fmt.Errorf("exclude-method %q: want package.Service/Method or one trailing *", p)
		}
	}
	return nil
}

type methodExclusions struct {
	patterns []string
	matched  []bool
}

func newMethodExclusions(o Options) (*methodExclusions, error) {
	if err := ValidateMethodExclusions(o.ExcludeMethods); err != nil {
		return nil, err
	}
	x := &methodExclusions{}
	for _, p := range o.ExcludeMethods {
		x.patterns = append(x.patterns, "/"+strings.TrimPrefix(p, "/"))
	}
	if !o.AllowUnsafeMethods {
		x.patterns = append(x.patterns, defaultMethodExclusion)
	}
	x.matched = make([]bool, len(x.patterns))
	return x, nil
}

func (x *methodExclusions) reason(method string) string {
	var reason string
	for i, p := range x.patterns {
		prefix := strings.TrimSuffix(p, "*")
		if method == p || (prefix != p && strings.HasPrefix(method, prefix)) {
			x.matched[i] = true
			if reason == "" {
				reason = "excluded by " + p
			}
		}
	}
	return reason
}

func skippedTask(protocol, name, reason string) task {
	return func(_ context.Context) Result {
		return Result{Protocol: protocol, Name: name, Class: Skipped, Detail: reason}
	}
}
