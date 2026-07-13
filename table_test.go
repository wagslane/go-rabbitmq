package rabbitmq

import "testing"

func TestTableToAMQPTableNested(t *testing.T) {
	table := Table{
		"top": "level",
		"nested": Table{
			"x-match": "all",
		},
		"list": []interface{}{
			Table{"deep": int32(1)},
			"plain",
		},
	}

	converted := tableToAMQPTable(table)
	if err := converted.Validate(); err != nil {
		t.Fatalf("converted table failed amqp validation: %v", err)
	}
}
