package connectionmanager

import "testing"

func Test_maskUrl(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "No username or password",
			url:      "amqp://localhost",
			expected: "amqp://localhost",
		},
		{
			name:     "With username and password",
			url:      "amqp://user:password@localhost",
			expected: "amqp://user:xxxxx@localhost",
		},
		{
			name:     "With username and password and query params",
			url:      "amqp://user:password@localhost?heartbeat=60",
			expected: "amqp://user:xxxxx@localhost?heartbeat=60",
		},
		{
			name:     "Invalid URL",
			url:      "invalidUrl",
			expected: "invalidUrl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if maskPassword(tt.url) != tt.expected {
				t.Errorf("masked password = %v, but wanted %v", maskPassword(tt.url), tt.expected)
			}
		})
	}
}
