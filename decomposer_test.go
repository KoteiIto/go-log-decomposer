package decomposer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecomposer(t *testing.T) {
	decomposer := NewDecomposer(func(option *Option) {
		option.IDGenerator = func() string {
			return "unique"
		}
	})
	logs := []string{
		`{"id":1,"table":"user","name":"user1","status":{"offence":100,"deffence":200,"skillType":"knight"},"tags":["fighter","ranker"],"items":[{"id":3,"type":"weapon","count":10},{"id":1,"type":"money","count":10000}]}`,
	}
	for _, log := range logs {
		decomposer.Do([]byte(log))
	}
	close(decomposer.logChan)

	actuals := []Event{}
	for len(actuals) < 6 {
		select {
		case event := <-decomposer.EventChan:
			actuals = append(actuals, event)
		case err := <-decomposer.ErrorChan:
			t.Error(t, err)
		}
	}

	expects := []Event{
		Event{
			table: "status",
			record: map[string]interface{}{
				"skill_type": "knight",
				"user_id":    "unique",
				"status_id":  "unique",
				"offence":    float64(100),
				"deffence":   float64(200),
			},
		},
		Event{
			table: "tags",
			record: map[string]interface{}{
				"index":   1,
				"user_id": "unique",
				"tags_id": "unique",
				"tags":    "fighter",
			},
		},
		Event{
			table: "tags",
			record: map[string]interface{}{
				"index":   2,
				"user_id": "unique",
				"tags_id": "unique",
				"tags":    "ranker",
			},
		},
		Event{
			table: "items",
			record: map[string]interface{}{
				"id":       float64(3),
				"type":     "weapon",
				"count":    float64(10),
				"index":    1,
				"user_id":  "unique",
				"items_id": "unique",
			},
		},
		Event{
			table: "items",
			record: map[string]interface{}{
				"id":       float64(1),
				"type":     "money",
				"count":    float64(10000),
				"index":    2,
				"user_id":  "unique",
				"items_id": "unique",
			},
		},
		Event{
			table: "user",
			record: map[string]interface{}{
				"table":   "user",
				"name":    "user1",
				"user_id": "unique",
				"id":      float64(1),
			},
		},
	}
	assert.ElementsMatch(t, expects, actuals)
}
