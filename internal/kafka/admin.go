package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicSpec describes a topic to ensure exists.
type TopicSpec struct {
	Name       string
	Partitions int32
	Replicas   int16
}

// EnsureTopics creates the given topics if they do not already exist. Existing
// topics are left untouched (partition/replica changes are not applied). This
// makes ingestion deterministic instead of relying on auto-creation, so all
// spans of a trace hash to the same partition by trace_id key.
func EnsureTopics(ctx context.Context, brokers []string, specs ...TopicSpec) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}

	// Skip topics that already exist so re-runs are idempotent.
	existing, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return err
	}
	for _, s := range specs {
		if existing.Has(s.Name) {
			continue
		}
		resp, err := adm.CreateTopic(ctx, s.Partitions, s.Replicas, nil, s.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return resp.Err
		}
	}
	return nil
}
