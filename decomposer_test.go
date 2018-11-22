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
	decomposer := NewDecomposer(func(decomposer *Decomposer) {
		decomposer.WorkerCount = 5
		decomposer.UIDGenerator = &mockUIDGenerator{}
	})
	logs := []string{
		`{"id":1,"name":"user1","status":{"offence":100,"deffence":200,"skillType":"knight"},"tags":["fighter","ranker"],"items":[{"id":3,"type":"weapon","count":10},{"id":1,"type":"money","count":10000}]}`,
	}
	for _, log := range logs {
		decomposer.EventEmitter <- NewInputEvent("user", []byte(log))
	}
	close(decomposer.EventEmitter)

	actuals := []*OutputEvent{}
	for len(actuals) < 6 {
		select {
		case event := <-decomposer.EventListener:
			actuals = append(actuals, event)
		case err := <-decomposer.ErrorListener:
			t.Error(t, err)
		}
	}

	expects := []*OutputEvent{
		&OutputEvent{
			Table: "user_status",
			Record: map[string]interface{}{
				"skill_type": "knight",
				"user_id":    "unique",
				"id":         "unique",
				"offence":    float64(100),
				"deffence":   float64(200),
			},
		},
		&OutputEvent{
			Table: "user_tags",
			Record: map[string]interface{}{
				"index":   1,
				"user_id": "unique",
				"id":      "unique",
				"value":   "fighter",
			},
		},
		&OutputEvent{
			Table: "user_tags",
			Record: map[string]interface{}{
				"index":   2,
				"user_id": "unique",
				"id":      "unique",
				"value":   "ranker",
			},
		},
		&OutputEvent{
			Table: "user_items",
			Record: map[string]interface{}{
				"id":            float64(3),
				"type":          "weapon",
				"count":         float64(10),
				"index":         1,
				"user_id":       "unique",
				"user_items_id": "unique",
			},
		},
		&OutputEvent{
			Table: "user_items",
			Record: map[string]interface{}{
				"id":            float64(1),
				"type":          "money",
				"count":         float64(10000),
				"index":         2,
				"user_id":       "unique",
				"user_items_id": "unique",
			},
		},
		&OutputEvent{
			Table: "user",
			Record: map[string]interface{}{
				"name":    "user1",
				"user_id": "unique",
				"id":      float64(1),
			},
		},
	}
	assert.ElementsMatch(t, expects, actuals)
}
