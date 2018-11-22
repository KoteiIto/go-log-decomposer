package decomposer

import (
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

type DefaultReplacementRule struct{}

func NewDefaultReplacementRule() *DefaultReplacementRule {
	return &DefaultReplacementRule{}
}

func (r *DefaultReplacementRule) Replace(s string) string {
	snake := matchFirstCap.ReplaceAllString(s, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
