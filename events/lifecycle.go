package events

import (
	"context"
	"encoding/json"
	"errors"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Lifecycle events are the agent's durable session lifecycle notifications on
// `easybus.session.lifecycle.<kind>`. They are part of the easybus surface so
// any NATS member (extensions) can subscribe idempotently. The publisher (the
// agent) sets a unique event id (eid); consumers dedup on it across durable
// replay / redelivery.

// Session lifecycle kinds (matches easybus.LifecycleEventKind).
const (
	Created = easybus.LifecycleEventKindCreated
	Forked  = easybus.LifecycleEventKindForked
	Renamed = easybus.LifecycleEventKindRenamed
	Deleted = easybus.LifecycleEventKindDeleted
)

// LifecycleSuccessFunc is called once per published lifecycle event.
type LifecycleSuccessFunc func(ctx context.Context, ev *easybus.LifecycleEvent) (bool, error)

// LifecyclePublishOpts carries bus publish options.
type LifecyclePublishOpts struct {
	// Bus impl detail: Publish has no ack; durable inbox publish does. We use
	// the protocol promise of an id-carrying envelope so consumers dedup.
	Inbox bool
}

// LifecyclePublish publishes one lifecycle event on easybus.session.lifecycle.<kind>.
// It always stamps a unique eid so consumers can dedup. If `inbox` is true it
// uses the durable inbox publish path (at-least-once).
func LifecyclePublish(ctx context.Context, b bus.Bus, tenant string, kind easybus.LifecycleEventKind, sessionName string, payload any, opts LifecyclePublishOpts) error {
	id := protocol.NewID()
	ev := &easybus.LifecycleEvent{
		Id:          &id,
		Kind:        kind,
		SessionName: sessionName,
		Payload:     easybus.Value(payload),
	}
	if kind == easybus.LifecycleEventKindRenamed {
		if m, ok := payload.(map[string]any); ok {
			if f, ok := m["from"].(string); ok {
				fv := f
				ev.From = &fv
			}
			if t, ok := m["to"].(string); ok {
				tv := t
				ev.To = &tv
			}
		}
	} else if kind == easybus.LifecycleEventKindForked {
		if m, ok := payload.(map[string]any); ok {
			if p, ok := m["parent"].(string); ok {
				pv := p
				ev.Parent = &pv
			}
		}
	}
	ch := protocol.ChLifecycle(tenant, easybus.LifecycleKindString(kind))
	if opts.Inbox {
		return b.InboxPublish(ctx, ch, ev, bus.InboxPublishOpts{ID: id, Tenant: tenant})
	}
	return b.Publish(ctx, ch, ev, bus.PublishOpts{Tenant: tenant})
}

// LifecycleConsumerSubscription is a durable lifecycle subscription.
type LifecycleConsumerSubscription struct {
	inner    bus.InboxSubscription
	handlers *lifecycleHandlers
	done     chan struct{}
}

// LifecycleHandlers routes each lifecycle event to per-kind callbacks.
type lifecycleHandlers struct {
	created func(ctx context.Context, ev *easybus.LifecycleEvent) error
	forked  func(ctx context.Context, ev *easybus.LifecycleEvent) error
	renamed func(ctx context.Context, ev *easybus.LifecycleEvent) error
	deleted func(ctx context.Context, ev *easybus.LifecycleEvent) error
}

// LifecycleHandlersOption configures a LifecycleConsumer.
type LifecycleHandlersOption func(*lifecycleHandlers)

// WithLifecycleCreated sets the created handler.
func WithLifecycleCreated(fn func(ctx context.Context, ev *easybus.LifecycleEvent) error) LifecycleHandlersOption {
	return func(h *lifecycleHandlers) { h.created = fn }
}

// WithLifecycleForked sets the forked handler.
func WithLifecycleForked(fn func(ctx context.Context, ev *easybus.LifecycleEvent) error) LifecycleHandlersOption {
	return func(h *lifecycleHandlers) { h.forked = fn }
}

// WithLifecycleRenamed sets the renamed handler.
func WithLifecycleRenamed(fn func(ctx context.Context, ev *easybus.LifecycleEvent) error) LifecycleHandlersOption {
	return func(h *lifecycleHandlers) { h.renamed = fn }
}

// WithLifecycleDeleted sets the deleted handler.
func WithLifecycleDeleted(fn func(ctx context.Context, ev *easybus.LifecycleEvent) error) LifecycleHandlersOption {
	return func(h *lifecycleHandlers) { h.deleted = fn }
}

// LifecycleConsume subscribes to durable lifecycle events for a session (or the
// wildcard ">" for all) and dispatches by kind. Each event is acked only after
// its handler returns nil; a non-nil handler result fails the delivery so the
// transport redelivers (and the consumer dedups by eid on re-handling).
func LifecycleConsume(ctx context.Context, b bus.Bus, sessionOrWildcard string, opts ...LifecycleHandlersOption) (*LifecycleConsumerSubscription, error) {
	h := &lifecycleHandlers{}
	for _, o := range opts {
		o(h)
	}
	// Subscribe on the mailbox wildcard for this session, or all sessions.
	subject := protocol.LifecycleWildcard + "*"
	if sessionOrWildcard != "" && sessionOrWildcard != "*" {
		subject = protocol.LifecycleWildcard + protocol.SessionToken(sessionOrWildcard)
	}
	inbox, err := b.InboxConsume(ctx, bus.InboxConsumeOpts{Subject: subject})
	if err != nil {
		return nil, err
	}
	sub := &LifecycleConsumerSubscription{inner: inbox, handlers: h, done: make(chan struct{})}
	go sub.loop(ctx)
	return sub, nil
}

func (s *LifecycleConsumerSubscription) loop(ctx context.Context) {
	defer close(s.done)
	for {
		msg, ok := s.inner.Next(ctx)
		if !ok {
			return
		}
		ev := decodeLifecyclePayload(msg.Envelope.Payload)
		if err := s.dispatch(ctx, ev); err != nil {
			// Fail the delivery so the transport redelivers; consumers dedup
			// by eid, so re-handling after the failure is safe.
			msg.Nak(1)
			continue
		}
		msg.Ack()
	}
}

func (s *LifecycleConsumerSubscription) dispatch(ctx context.Context, ev *easybus.LifecycleEvent) error {
	switch ev.Kind {
	case easybus.LifecycleEventKindCreated:
		if s.handlers.created != nil {
			return s.handlers.created(ctx, ev)
		}
	case easybus.LifecycleEventKindForked:
		if s.handlers.forked != nil {
			return s.handlers.forked(ctx, ev)
		}
	case easybus.LifecycleEventKindRenamed:
		if s.handlers.renamed != nil {
			return s.handlers.renamed(ctx, ev)
		}
	case easybus.LifecycleEventKindDeleted:
		if s.handlers.deleted != nil {
			return s.handlers.deleted(ctx, ev)
		}
	default:
		return nil
	}
	return nil
}

func decodeLifecyclePayload(v any) *easybus.LifecycleEvent {
	if m, ok := v.(map[string]any); ok {
		ev := &easybus.LifecycleEvent{}
		if id, ok := m["id"].(string); ok {
			ev.Id = &id
		}
		if k, ok := m["kind"].(string); ok {
			ev.Kind = easybus.LifecycleKindFromString(k)
		}
		if sn, ok := m["session_name"].(string); ok {
			ev.SessionName = sn
		}
		if p, ok := m["parent"].(string); ok {
			ev.Parent = &p
		}
		if f, ok := m["from"].(string); ok {
			ev.From = &f
		}
		if t, ok := m["to"].(string); ok {
			ev.To = &t
		}
		ev.Payload = easybus.Value(m["payload"])
		return ev
	}
	var ev easybus.LifecycleEvent
	if s, ok := v.(string); ok {
		_ = json.Unmarshal([]byte(s), &ev)
	}
	return &ev
}

// Close stops the subscription.
func (s *LifecycleConsumerSubscription) Close() error {
	s.inner.Close()
	<-s.done
	return nil
}

var errUnknownLifecycle = errors.New("unknown lifecycle kind")
