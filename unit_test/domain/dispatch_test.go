package domain_test

import (
	"testing"
	"time"

	"notifier/internal/domain"
)

func TestMQ06_DeliveryWindowNeverEarly(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		at   time.Time
		want string
	}{
		{now.Add(-time.Second), domain.WindowImmediate},
		{now, domain.WindowImmediate},
		{now.Add(time.Hour), domain.WindowDelayed},
		{now.Add(48 * time.Hour), domain.WindowDefer},
	}
	for _, c := range cases {
		got, deliverAt, next, err := domain.DeliveryWindow(now, c.at, 24*time.Hour, time.Second)
		if err != nil || got != c.want {
			t.Fatalf("%v: %s %v", c.at, got, err)
		}
		if got == domain.WindowDelayed && !deliverAt.Equal(c.at) {
			t.Fatal("deliver_at must equal schedule")
		}
		if got == domain.WindowDefer && !next.Equal(c.at.Add(-24*time.Hour).Add(time.Second)) {
			t.Fatalf("next %v", next)
		}
	}
}

func TestMQ08_ParseCommandRejectsUnknownAndExtra(t *testing.T) {
	ok := `{"protocol_version":1,"outbox_id":"3","message_id":"o1_3","event_id":"1","delivery_id":"2","round_no":1,"schedule_version":1}`
	if _, err := domain.ParseCommand([]byte(ok)); err != nil {
		t.Fatal(err)
	}
	bad := []string{
		`{"protocol_version":2,"outbox_id":"3","message_id":"o1_3","event_id":"1","delivery_id":"2","round_no":1,"schedule_version":1}`,
		`{"protocol_version":1,"outbox_id":"3","message_id":"o1_3","event_id":"1","delivery_id":"2","round_no":1,"schedule_version":1,"x":1}`,
		`{"protocol_version":1,"outbox_id":"3","outbox_id":"3","message_id":"o1_3","event_id":"1","delivery_id":"2","round_no":1}`,
		ok + `{}`,
	}
	for _, b := range bad {
		if _, err := domain.ParseCommand([]byte(b)); err == nil {
			t.Fatalf("accepted %s", b)
		}
	}
}

func TestMQ05_PublishBackoffBounded(t *testing.T) {
	if d := domain.PublishBackoff(1, time.Second, time.Minute, 1); d != time.Second {
		t.Fatal(d)
	}
	if d := domain.PublishBackoff(30, time.Second, time.Minute, 1); d != time.Minute {
		t.Fatal(d)
	}
	if d := domain.PublishBackoff(3, time.Second, time.Minute, 0.1); d != 2*time.Second {
		t.Fatal(d)
	}
}
