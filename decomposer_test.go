package decomposer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockUIDGenerator struct{}

func (m *mockUIDGenerator) Generate(_ string) interface{} {
	return "unique"
}

func TestDecomposer(t *testing.T) {
	type Case struct {
		message string
		input   struct {
			initialize func(decomposer *Decomposer)
			logs       []string
		}
		expect []*OutputEvent
	}

	cases := []Case{
		{
			message: "Can Decompose simple log",
			input: struct {
				initialize func(decomposer *Decomposer)
				logs       []string
			}{
				initialize: func(decomposer *Decomposer) {
					decomposer.UIDGenerator = &mockUIDGenerator{}
				},
				logs: []string{
					`
{
	"id": 1,
	"name": "user1"
}
`,
				},
			},
			expect: []*OutputEvent{
				&OutputEvent{
					Name: "project/user",
					Record: map[string]interface{}{
						"name":    "user1",
						"user_id": "unique",
						"id":      float64(1),
					},
				},
			},
		},
		{
			message: "Can Decompose nested log",
			input: struct {
				initialize func(decomposer *Decomposer)
				logs       []string
			}{
				initialize: func(decomposer *Decomposer) {
					decomposer.UIDGenerator = &mockUIDGenerator{}
				},
				logs: []string{
					`
{
	"id": 1,
	"name": "user1",
	"status": {
		"offence": 100,
		"deffence": 200,
		"skillType": "knight"
	}
}
`,
				},
			},
			expect: []*OutputEvent{
				&OutputEvent{
					Name: "project/user",
					Record: map[string]interface{}{
						"name":    "user1",
						"user_id": "unique",
						"id":      float64(1),
					},
				},
				&OutputEvent{
					Name: "project/user_status",
					Record: map[string]interface{}{
						"skill_type": "knight",
						"user_id":    "unique",
						"id":         "unique",
						"offence":    float64(100),
						"deffence":   float64(200),
					},
				},
			},
		},
		{
			message: "Can Decompose array log",
			input: struct {
				initialize func(decomposer *Decomposer)
				logs       []string
			}{
				initialize: func(decomposer *Decomposer) {
					decomposer.UIDGenerator = &mockUIDGenerator{}
				},
				logs: []string{
					`
{
	"id": 1,
	"name": "user1",
	"tags": [
		"fighter",
		"ranker"
	]
}
`,
				},
			},
			expect: []*OutputEvent{
				&OutputEvent{
					Name: "project/user",
					Record: map[string]interface{}{
						"name":    "user1",
						"user_id": "unique",
						"id":      float64(1),
					},
				},
				&OutputEvent{
					Name: "project/user_tags",
					Record: map[string]interface{}{
						"index":   0,
						"user_id": "unique",
						"id":      "unique",
						"value":   "fighter",
					},
				},
				&OutputEvent{
					Name: "project/user_tags",
					Record: map[string]interface{}{
						"index":   1,
						"user_id": "unique",
						"id":      "unique",
						"value":   "ranker",
					},
				},
			},
		},
		{
			message: "Can Decompose complex log",
			input: struct {
				initialize func(decomposer *Decomposer)
				logs       []string
			}{
				initialize: func(decomposer *Decomposer) {
					decomposer.WorkerCount = 5
					decomposer.UIDGenerator = &mockUIDGenerator{}
				},
				logs: []string{
					`
{
	"id": 1,
	"name": "user1",
	"status": {
		"offence": 100,
		"deffence": 200,
		"skillType": "knight"
	},
	"tags": [
		"fighter",
		"ranker"
	],
	"items": [
		{
			"id": 3,
			"type": "weapon",
			"count": 10
		},
		{
			"id": 1,
			"type": "money",
			"count": 10000
		}
	]
}
`,
				},
			},
			expect: []*OutputEvent{
				&OutputEvent{
					Name: "project/user_status",
					Record: map[string]interface{}{
						"skill_type": "knight",
						"user_id":    "unique",
						"id":         "unique",
						"offence":    float64(100),
						"deffence":   float64(200),
					},
				},
				&OutputEvent{
					Name: "project/user_tags",
					Record: map[string]interface{}{
						"index":   0,
						"user_id": "unique",
						"id":      "unique",
						"value":   "fighter",
					},
				},
				&OutputEvent{
					Name: "project/user_tags",
					Record: map[string]interface{}{
						"index":   1,
						"user_id": "unique",
						"id":      "unique",
						"value":   "ranker",
					},
				},
				&OutputEvent{
					Name: "project/user_items",
					Record: map[string]interface{}{
						"id":            float64(3),
						"type":          "weapon",
						"count":         float64(10),
						"index":         0,
						"user_id":       "unique",
						"user_items_id": "unique",
					},
				},
				&OutputEvent{
					Name: "project/user_items",
					Record: map[string]interface{}{
						"id":            float64(1),
						"type":          "money",
						"count":         float64(10000),
						"index":         1,
						"user_id":       "unique",
						"user_items_id": "unique",
					},
				},
				&OutputEvent{
					Name: "project/user",
					Record: map[string]interface{}{
						"name":    "user1",
						"user_id": "unique",
						"id":      float64(1),
					},
				},
			},
		},
		{
			message: "Can Replace to snake case",
			input: struct {
				initialize func(decomposer *Decomposer)
				logs       []string
			}{
				initialize: func(decomposer *Decomposer) {
					decomposer.UIDGenerator = &mockUIDGenerator{}
				},
				logs: []string{
					`
{
	"StatusID": 1,
	"itemId": 2
}
`,
				},
			},
			expect: []*OutputEvent{
				&OutputEvent{
					Name: "project/user",
					Record: map[string]interface{}{
						"status_id": float64(1),
						"id":        "unique",
						"item_id":   float64(2),
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.message, func(t *testing.T) {
			decomposer := NewDecomposer(c.input.initialize)
			for _, log := range c.input.logs {
				decomposer.EventEmitter <- NewInputEvent("project/user", []byte(log))
			}
			close(decomposer.EventEmitter)
			actuals := []*OutputEvent{}
			for len(actuals) < len(c.expect) {
				select {
				case event := <-decomposer.EventListener:
					actuals = append(actuals, event)
				case err := <-decomposer.ErrorListener:
					t.Error(t, err)
				}
			}
			assert.ElementsMatch(t, c.expect, actuals)
		})
	}

}
