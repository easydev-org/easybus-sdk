package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	easybus "github.com/easydev-org/easybus-sdk"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/identity"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	streamMailbox = "EASYBUS_MAILBOX"
	streamEvents  = "EASYBUS_EVENTS"
	streamDLQ     = "EASYBUS_DLQ"
	// streamChannels carries channel events. A stream is REQUIRED, not optional: a
	// js.Publish to a subject no stream claims is not retained, and a core NATS subscriber
	// never sees it either — the message simply goes nowhere, with no error at publish time.
	streamChannels  = "EASYBUS_CHANNELS"
	channelWildcard = "easybus.*.channel.>"
	inboxWildcard   = "easybus.*.mailbox.>"
	eventsWildcard  = "easybus.*.session.events.>"
	// EASYBUS_EVENTS also carries session lifecycle + session-changed (matches the
	// TS SDK). Omitting these made ensureStreams DOWNGRADE the stream to only
	// events, dropping the agent's session.changed/lifecycle publishes
	// (JetStreamNotEnabled: no stream covers the subject).
	lifecycleWildcard = "easybus.*.session.lifecycle.>"
	changedWildcard   = "easybus.*.session.changed"
	dlqWildcard       = "easybus.*.dlq.>"
	durableConsumer   = "easybus-mailbox-push"
	queueGroup        = "easybus-mailbox"
	objectBucket      = "EASYBUS_TOOL"
	objectBucketDur   = "EASYBUS_FILES"
)

// Bus is the NATS transport adapter.
type Bus struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	idn *identity.Identity
	// durable, when set, owns ObjectPutPersistent/ObjectGetPersistent (file
	// bytes). Transient objects always stay on NATS. One backend, no fallback.
	durable bus.ObjectStore
}

var _ bus.Bus = (*Bus)(nil)

// Options tune stream topology and retention at connect time.
type Options struct {
	// MaxAge bounds stream retention. Zero means the default (24h).
	MaxAge time.Duration
	// Replicas sets the JetStream replica count at stream creation.
	// Zero means the server default (1). Cannot change after creation.
	Replicas int
	// Identity enables opt-in message authentication: outgoing messages
	// carry easybus-id/easybus-sig NATS headers (HMAC), incoming messages are
	// verified. Nil (default) = zero auth overhead, everything passes.
	Identity *identity.Identity
	// DurableObjects, when set, owns the durable (file-bytes) object
	// methods so they never touch NATS; transient objects stay on NATS.
	// One backend per class, no read fallback.
	DurableObjects bus.ObjectStore
}

// Connect establishes a NATS connection with the default stream topology.
func Connect(url string) (*Bus, error) {
	return ConnectWithOptions(url, Options{})
}

// ConnectWithOptions establishes a NATS connection and reconciles the
// protocol stream topology to the desired state (declarative):
//   - a missing stream is created (retention/replicas from Options)
//   - an existing stream with drifted subjects is narrowed to the desired
//     set (this subsumes the 0.1->0.2 mailbox re-layout: the legacy stream
//     that also captured session events is narrowed so EASYBUS_EVENTS can hold)
//
// The fleet never crashes on a layout change — drift is repaired, not fatal.
func ConnectWithOptions(url string, opts Options) (*Bus, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if err := ensureStreams(js, opts); err != nil {
		nc.Close()
		return nil, err
	}
	return &Bus{nc: nc, js: js, idn: opts.Identity, durable: opts.DurableObjects}, nil
}

// ensureStreams reconciles the mailbox/events/dlq streams. Order matters:
// the mailbox is narrowed FIRST so the events stream (whose subject the
// legacy mailbox used to also capture) can then be created without an
// overlap error.
func ensureStreams(js nats.JetStreamContext, opts Options) error {
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = 24 * time.Hour
	}
	specs := []struct {
		name     string
		subjects []string
	}{
		// Channels first: this subject space is disjoint from the others, so ordering does
		// not matter for correctness — but declaring it here is what makes channel events
		// persist at all. Without a stream, js.Publish silently retains nothing.
		{streamChannels, []string{channelWildcard}},
		{streamMailbox, []string{inboxWildcard}},
		{streamEvents, []string{eventsWildcard, lifecycleWildcard, changedWildcard}},
		{streamDLQ, []string{dlqWildcard}},
	}
	for _, s := range specs {
		info, err := js.StreamInfo(s.name)
		if err != nil {
			cfg := &nats.StreamConfig{Name: s.name, Subjects: s.subjects, MaxAge: maxAge}
			if opts.Replicas > 0 {
				cfg.Replicas = opts.Replicas
			}
			if _, aerr := js.AddStream(cfg); aerr != nil {
				return fmt.Errorf("add stream %s: %w", s.name, aerr)
			}
			continue
		}
		if !sameSubjects(info.Config.Subjects, s.subjects) {
			cfg := info.Config
			cfg.Subjects = s.subjects
			if _, uerr := js.UpdateStream(&cfg); uerr != nil {
				return fmt.Errorf("reconcile stream %s subjects: %w", s.name, uerr)
			}
		}
	}
	return nil
}

// signMsg attaches the identity HMAC to a NATS message's headers (opt-in;
// nil identity = no-op).
func (b *Bus) signMsg(msg *nats.Msg, ch, kind, id string, payload any) {
	if b.idn == nil {
		return
	}
	h := identity.AuthHeader(*b.idn, identity.Fields{Tenant: envelopeTenant(msg.Data), Ch: ch, Kind: kind, ID: id, Payload: payload})
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set("easybus-id", h.ID)
	msg.Header.Set("easybus-sig", h.Sig)
}

// verifyMsg checks easybus-id/easybus-sig headers (opt-in; nil identity = always
// true). Returns false for missing/invalid signatures.
func (b *Bus) verifyMsg(m *nats.Msg, raw *rawEnvelope) bool {
	if b.idn == nil {
		return true
	}
	sig := m.Header.Get("easybus-sig")
	if sig == "" {
		return false
	}
	id := ""
	if raw.ID != nil {
		id = *raw.ID
	}
	return identity.Verify(m.Header.Get("easybus-id"), b.idn.Secret,
		identity.Fields{Tenant: raw.Tenant, Ch: m.Subject, Kind: raw.Kind, ID: id, Payload: json.RawMessage(raw.Payload)}, sig)
}

// envelopeTenant reads just the tenant field out of an already-encoded
// envelope (used for signing).
func envelopeTenant(data []byte) string {
	var raw struct {
		Tenant string `json:"tenant"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return ""
	}
	return raw.Tenant
}

func sameSubjects(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]bool{}
	for _, s := range a {
		seen[s] = true
	}
	for _, s := range b {
		if !seen[s] {
			return false
		}
	}
	return true
}

// streamFor routes a (possibly wildcard) subject to its stream by the v2
// layout easybus.<tenant>.<area>...: session.events / session.lifecycle /
// session.changed -> EVENTS, dlq -> DLQ, everything else -> MAILBOX. Matches
// the TS SDK so the two never fight over the EASYBUS_EVENTS subject set.
func streamFor(subject string) string {
	seg := strings.Split(subject, ".")
	if len(seg) > 3 && seg[2] == "session" &&
		(seg[3] == "events" || seg[3] == "lifecycle" || seg[3] == "changed") {
		return streamEvents
	}
	if len(seg) > 2 && seg[2] == "dlq" {
		return streamDLQ
	}
	return streamMailbox
}

// dlqSubjectFor maps an original queue subject to its tenant dead-letter
// subject, preserving the tenant segment (easybus.<tenant>.dlq.<token>).
func dlqSubjectFor(subject string) string {
	token := subject
	if i := strings.LastIndex(subject, "."); i >= 0 {
		token = subject[i+1:]
	}
	if tenant := protocol.SubjectTenant(subject); tenant != "" {
		return protocol.TenantPrefix(tenant) + "dlq." + token
	}
	return "easybus.dlq." + token
}

func encode(payload any) ([]byte, error) { return json.Marshal(payload) }

// tenantOf resolves an outgoing message's tenant: explicit when given, else
// the subject's tenant segment (data-plane subjects always carry one).
func tenantOf(ch, explicit string) string {
	if explicit != "" {
		return explicit
	}
	return protocol.SubjectTenant(ch)
}

// buildEnvelope assembles the wire envelope in one place. `tenant` is
// REQUIRED on the v2 wire. The optional fields ride only when set.
func buildEnvelope(kind, ch string, payload any, tenant, id, sessionName, replyTo string) ([]byte, error) {
	m := map[string]any{"v": 2, "ch": ch, "kind": kind, "tenant": tenant, "payload": payload}
	if id != "" {
		m["id"] = id
	}
	if sessionName != "" {
		m["session_name"] = sessionName
	}
	if replyTo != "" {
		m["reply_to"] = replyTo
	}
	return json.Marshal(m)
}

// rawEnvelope mirrors the wire shape but keeps payload as raw bytes, so
// downstream Coerce calls skip a full Marshal round trip (the hot path:
// every tool call / hook / config delivery).
type rawEnvelope struct {
	V           int             `json:"v"`
	Tenant      string          `json:"tenant"`
	Ch          string          `json:"ch"`
	Kind        string          `json:"kind"`
	ID          *string         `json:"id,omitempty"`
	SessionName *string         `json:"session_name,omitempty"`
	ReplyTo     *string         `json:"reply_to,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

func decode(m *nats.Msg) (*easybus.Envelope, error) {
	var raw rawEnvelope
	if err := json.Unmarshal(m.Data, &raw); err != nil {
		return nil, err
	}
	// v2 is a breaking layout change: reject anything that is not v2.
	if raw.V != 2 {
		log.Printf("[easybus] rejecting envelope version %d on %s (this build speaks v2)", raw.V, raw.Ch)
		return nil, fmt.Errorf("unsupported envelope version %d", raw.V)
	}
	// Envelope tenant must agree with the subject's tenant segment.
	if expected := protocol.SubjectTenant(m.Subject); expected != "" && raw.Tenant != expected {
		return nil, fmt.Errorf("envelope tenant %q != subject tenant %q on %s", raw.Tenant, expected, m.Subject)
	}
	v := int32(raw.V)
	var payload *structpb.Value
	if len(raw.Payload) > 0 {
		payload = &structpb.Value{}
		if json.Unmarshal(raw.Payload, payload) != nil {
			payload = nil
		}
	}
	env := &easybus.Envelope{
		V:           &v,
		Tenant:      raw.Tenant,
		Ch:          raw.Ch,
		Kind:        easybus.EnvelopeKindFromString(raw.Kind),
		Id:          raw.ID,
		SessionName: raw.SessionName,
		Payload:     payload,
	}
	if m.Reply != "" {
		env.ReplyTo = &m.Reply
	}
	return env, nil
}

func (b *Bus) Request(ctx context.Context, ch string, payload any, opts bus.RequestOpts) (*easybus.Envelope, error) {
	data, err := buildEnvelope("req", ch, payload, tenantOf(ch, opts.Tenant), "", opts.SessionName, "")
	if err != nil {
		return nil, err
	}
	// TimeoutMs > 0 bounds the request; 0 means no timeout (ctx only).
	var cancel context.CancelFunc
	if opts.TimeoutMs > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(opts.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	var m *nats.Msg
	if b.idn != nil {
		rm := nats.NewMsg(ch)
		rm.Data = data
		b.signMsg(rm, ch, "req", "", payload)
		m, err = b.nc.RequestMsgWithContext(ctx, rm)
		if err != nil {
			return nil, err
		}
	} else {
		m, err = b.nc.RequestWithContext(ctx, ch, data)
		if err != nil {
			return nil, err
		}
	}
	if b.idn != nil {
		var raw rawEnvelope
		if json.Unmarshal(m.Data, &raw) == nil && !b.verifyMsg(m, &raw) {
			return nil, fmt.Errorf("identity: signature verification failed on %s", m.Subject)
		}
	}
	return decode(m)
}

func (b *Bus) RequestMany(ctx context.Context, ch string, payload any, opts bus.RequestOpts) ([]*easybus.Envelope, error) {
	data, err := buildEnvelope("req", ch, payload, tenantOf(ch, opts.Tenant), "", "", "")
	if err != nil {
		return nil, err
	}
	inbox := nats.NewInbox()
	sub, err := b.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	msg := nats.NewMsg(ch)
	msg.Data = data
	msg.Reply = inbox
	if b.idn != nil {
		b.signMsg(msg, ch, "req", "", payload)
	}
	if err := b.nc.PublishMsg(msg); err != nil {
		return nil, err
	}

	maxWait := opts.MaxWaitMs
	if maxWait == 0 {
		maxWait = 500
	}
	out := []*easybus.Envelope{}
	for {
		m, err := sub.NextMsg(time.Duration(maxWait) * time.Millisecond)
		if err != nil {
			return out, nil
		}
		if b.idn != nil {
			var raw rawEnvelope
			if json.Unmarshal(m.Data, &raw) == nil && !b.verifyMsg(m, &raw) {
				continue
			}
		}
		env, err := decode(m)
		if err != nil {
			continue
		}
		out = append(out, env)
	}
}

func (b *Bus) Publish(ctx context.Context, ch string, payload any, opts bus.PublishOpts) error {
	data, err := buildEnvelope("pub", ch, payload, tenantOf(ch, opts.Tenant), "", "", opts.ReplyTo)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(ch)
	msg.Data = data
	if opts.ReplyTo != "" {
		msg.Reply = opts.ReplyTo
	}
	b.signMsg(msg, ch, "pub", "", payload)
	return b.nc.PublishMsg(msg)
}

func (b *Bus) Subscribe(ctx context.Context, ch string, opts bus.SubscribeOpts) (bus.Subscription, error) {
	var sub *nats.Subscription
	var err error
	if opts.Queue != "" {
		sub, err = b.nc.QueueSubscribeSync(ch, opts.Queue)
	} else {
		sub, err = b.nc.SubscribeSync(ch)
	}
	if err != nil {
		return nil, err
	}
	// SubscribeSync is subject to an async-subscription race: a publish issued
	// right after Subscribe may land before NATS registers interest on the
	// server. Flushing the connection pins the subscription.
	if err := b.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return &natsSub{sub: sub, bus: b}, nil
}

type natsSub struct {
	sub *nats.Subscription
	bus *Bus
}

func (s *natsSub) Next(ctx context.Context) (*easybus.Envelope, bool) {
	m, err := s.sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, false
	}
	if s.bus.idn != nil {
		var raw rawEnvelope
		if json.Unmarshal(m.Data, &raw) == nil && !s.bus.verifyMsg(m, &raw) {
			return nil, false // bad/missing signature: drop
		}
	}
	env, err := decode(m)
	if err != nil {
		return nil, false
	}
	return env, true
}

func (s *natsSub) Close() error { return s.sub.Unsubscribe() }

func (b *Bus) InboxPublish(ctx context.Context, ch string, payload any, opts bus.InboxPublishOpts) error {
	data, err := buildEnvelope("queue", ch, payload, tenantOf(ch, opts.Tenant), opts.ID, opts.SessionName, "")
	if err != nil {
		return err
	}
	_, err = b.js.Publish(ch, data, nats.MsgId(opts.ID))
	return err
}

func (b *Bus) InboxConsume(ctx context.Context, opts bus.InboxConsumeOpts) (bus.InboxSubscription, error) {
	subject := opts.Subject
	if subject == "" {
		subject = inboxWildcard
	}
	// An explicit pull consumer per distinct subject filter. A durable
	// consumer's filter_subject is fixed at creation, so reusing one durable
	// name across different subjects silently delivers nothing — derive a
	// stable per-subject durable name instead.
	durable := durableConsumer
	if opts.Durable != "" {
		durable = opts.Durable
	} else if subject != inboxWildcard {
		durable = durableConsumer + "-" + protocol.SessionToken(subject)
	}
	// PullSubscribe already carries the durable NAME (it is its second argument), so it must not
	// be supplied again as an option: nats.go rejects "option Durable set more than once".
	//
	// InactiveThreshold IS worth stating. Without it the server default applies, and a consumer
	// that is meant to outlive a quiet period can be reclaimed — which is indistinguishable, from
	// `consumers_info` a minute later, from the consumer never having been created.
	subOpts := []nats.SubOpt{
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.InactiveThreshold(24 * time.Hour),
	}
	// No queue group here: a durable PULL consumer already distributes its messages across
	// whoever is pulling from it, so several replicas sharing one durable name split the
	// stream without any extra configuration. InboxConsumeOpts.Queue is accepted for callers
	// that want the intent recorded, but the split is the durable name's doing.
	sub, err := b.js.PullSubscribe(subject, durable, subOpts...)
	if err != nil {
		return nil, err
	}
	return &inboxSub{js: b.js, subject: subject, durable: durable, subOpts: subOpts, sub: sub}, nil
}

// fetch pulls one message, holding the lock so a concurrent recreate cannot swap the
// subscription mid-Fetch.
func (s *inboxSub) fetch() ([]*nats.Msg, error) {
	s.mu.Lock()
	sub := s.sub
	s.mu.Unlock()
	if sub == nil {
		return nil, nats.ErrBadSubscription
	}
	return sub.Fetch(1, nats.MaxWait(2*time.Second))
}

// recreate re-opens the pull subscription after the consumer was removed underneath us.
//
// WHY this exists: a durable consumer can be deleted while we hold no connection — an operator
// cleaning up, a stream reset, or the server reclaiming it. `Fetch` then fails forever with
// "consumer not found", and every caller sees a dead consumer that it has no way to repair.
func (s *inboxSub) recreate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub != nil {
		_ = s.sub.Unsubscribe()
		s.sub = nil
	}
	sub, err := s.js.PullSubscribe(s.subject, s.durable, s.subOpts...)
	if err != nil {
		return err
	}
	s.sub = sub
	return nil
}

// isConsumerGone reports whether an error means the consumer no longer exists.
//
// Matched on the message because the nats.go client does not export a typed error for this: the
// server answers 404 "consumer not found" and the client surfaces it as plain text.
func isConsumerGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrConsumerNotFound) || errors.Is(err, nats.ErrBadSubscription) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "consumer not found") ||
		strings.Contains(msg, "consumer deleted") ||
		strings.Contains(msg, "no responders")
}

type inboxSub struct {
	js      nats.JetStreamContext
	subject string
	durable string
	subOpts []nats.SubOpt

	mu  sync.Mutex
	sub *nats.Subscription
}

func (s *inboxSub) Next(ctx context.Context) (*bus.InboxMsg, bool) {
	// PullSubscription requires Fetch(); NextMsgWithContext rejects pull subs.
	msgs, err := s.fetch()
	if err != nil {
		// An ERROR is not "nothing to read", and conflating them is what produced a consumer that
		// spun millions of times per minute while reporting nothing: `Fetch` fails IMMEDIATELY
		// when the consumer is gone, so the caller's idle branch re-fetched in a tight loop and
		// the durable consumer stayed missing. Say which it is.
		if isConsumerGone(err) {
			log.Printf("[easybus] inbox consumer %q is gone (%v); recreating", s.durable, err)
			if rerr := s.recreate(); rerr != nil {
				log.Printf("[easybus] inbox consumer %q recreate failed: %v", s.durable, rerr)
				// Back off rather than spin: the caller treats this as idle, and an immediate
				// retry against a broken broker would burn the CPU for no progress.
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
			}
			return nil, false
		}
		if errors.Is(err, nats.ErrTimeout) {
			return nil, false // genuinely idle: no message within the wait
		}
		log.Printf("[easybus] inbox fetch on %q: %v", s.subject, err)
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
		return nil, false
	}
	if len(msgs) == 0 {
		return nil, false
	}
	m := msgs[0]
	env, err := decode(m)
	if err != nil {
		return nil, false
	}
	msg := &bus.InboxMsg{Envelope: env}
	msg.SetHandlers(
		func() { _ = m.Ack() },
		func(delayMs int) { _ = m.NakWithDelay(time.Duration(delayMs) * time.Millisecond) },
		// Term copies the message to the dead-letter stream first; use
		// TermNoDLQ to discard it outright.
		func() {
			if id := env.Id; id != nil {
				_, _ = s.js.Publish(dlqSubjectFor(m.Subject), m.Data, nats.MsgId(*id))
			} else {
				_, _ = s.js.Publish(dlqSubjectFor(m.Subject), m.Data)
			}
			_ = m.Term()
		},
		func() { _ = m.Term() },
	)
	return msg, true
}

func (s *inboxSub) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub == nil {
		return nil
	}
	err := s.sub.Unsubscribe()
	s.sub = nil
	return err
}

// InboxPurge deletes every retained message matching the channel filter.
//
// Callers pass the same shape they subscribe with, including the exported
// wildcards, which already end in '.' (`MailboxWildcardAll` is
// "easybus.*.mailbox."). Appending ".>" blindly produced "…mailbox..>", a filter
// that matches nothing — and the purge still reported success while the stream
// kept every message. That silent no-op is why this completes the filter
// deliberately instead of concatenating.
func (b *Bus) InboxPurge(ctx context.Context, ch string) error {
	if ch == "" {
		return fmt.Errorf("purge: channel is required")
	}
	filter := ch
	switch {
	case strings.HasSuffix(filter, ">"):
		// already a full wildcard
	case strings.HasSuffix(filter, "."):
		filter += ">"
	default:
		filter += ".>"
	}
	stream := streamFor(ch)
	// Subject narrows the purge to this channel: the mailbox and DLQ streams are
	// shared by every tenant, so an unfiltered purge would wipe them.
	if err := b.js.PurgeStream(stream, &nats.StreamPurgeRequest{Subject: filter}); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil // nothing has been published there yet
		}
		return fmt.Errorf("purge %s on %s: %w", filter, stream, err)
	}
	return nil
}

func (b *Bus) ObjectPut(ctx context.Context, name string, data []byte) error {
	os, err := b.js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: objectBucket, TTL: 24 * 3600 * 1e9})
	if err != nil {
		// Bucket exists (possibly with another config) — open it.
		os, err = b.js.ObjectStore(objectBucket)
		if err != nil {
			return err
		}
	}
	_, err = os.PutBytes(name, data)
	return err
}

func (b *Bus) ObjectGet(ctx context.Context, name string) ([]byte, error) {
	os, err := b.js.ObjectStore(objectBucket)
	if err != nil {
		return nil, nil
	}
	return os.GetBytes(name)
}

func (b *Bus) ObjectPutPersistent(ctx context.Context, name string, data []byte) error {
	if b.durable != nil {
		return b.durable.ObjectPutPersistent(ctx, name, data)
	}
	os, err := b.js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: objectBucketDur})
	if err != nil {
		os, err = b.js.ObjectStore(objectBucketDur)
		if err != nil {
			return err
		}
	}
	_, err = os.PutBytes(name, data)
	return err
}

func (b *Bus) ObjectGetPersistent(ctx context.Context, name string) ([]byte, error) {
	if b.durable != nil {
		return b.durable.ObjectGetPersistent(ctx, name)
	}
	os, err := b.js.ObjectStore(objectBucketDur)
	if err != nil {
		return nil, nil
	}
	return os.GetBytes(name)
}

// kvStore creates-or-opens a KV bucket. CreateKeyValue errors when the
// bucket exists with a different config (e.g. a different per-call TTL), so
// fall back to opening the existing bucket.
func (b *Bus) kvStore(bucket string, ttlMs int64) (nats.KeyValue, error) {
	kv, err := b.js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket, TTL: time.Duration(ttlMs) * time.Millisecond})
	if err != nil {
		return b.js.KeyValue(bucket)
	}
	return kv, nil
}

func (b *Bus) KVCreate(ctx context.Context, bucket, key, value string, ttlMs int64) (int64, error) {
	kv, err := b.kvStore(bucket, ttlMs)
	if err != nil {
		return 0, err
	}
	rev, err := kv.Create(key, []byte(value))
	if err != nil {
		return 0, nil
	}
	return int64(rev), nil
}

func (b *Bus) KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error {
	kv, err := b.kvStore(bucket, ttlMs)
	if err != nil {
		return err
	}
	_, err = kv.Put(key, []byte(value))
	return err
}

// KvWatch streams bucket entries matching keys (NATS wildcard). Snapshot
// entries arrive first (IsUpdate=false), then live updates; the channel
// closes when the cancel func runs or ctx ends.
func (b *Bus) KvWatch(ctx context.Context, bucket, keys string) (<-chan bus.KvEvent, func(), error) {
	// Create the bucket if it does not exist yet.
	//
	// A watcher is usually the FIRST thing to touch a bucket: an agent starts watching
	// subscriptions before any session has been created and written one. Failing here with
	// "bucket not found" meant the watch never started, the index stayed empty forever, and
	// every later write went into a bucket nobody was reading — with only a single error log
	// at boot to show for it.
	kv, err := b.kvStore(bucket, 0)
	if err != nil {
		return nil, nil, err
	}
	w, err := kv.Watch(keys)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan bus.KvEvent, 32)
	stop := func() { _ = w.Stop() }
	go func() {
		defer close(ch)
		snapshotDone := false
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-w.Updates():
				if !ok {
					return
				}
				if e == nil {
					snapshotDone = true
					select {
					case ch <- bus.KvEvent{Done: true}:
					case <-ctx.Done():
						return
					}
					continue
				}
				ev := bus.KvEvent{
					Key:      e.Key(),
					Revision: e.Revision(),
					IsUpdate: snapshotDone,
				}
				if e.Operation() == nats.KeyValuePut {
					ev.Value = string(e.Value())
				} else {
					ev.Deleted = true
				}
				select {
				case ch <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, stop, nil
}

func (b *Bus) KVGet(ctx context.Context, bucket, key string) (string, error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return "", nil
	}
	e, err := kv.Get(key)
	if err != nil {
		// KeyNotFound includes tombstones from a prior Delete — the value is
		// gone either way, so a miss ("" ,nil) is the honest answer.
		return "", nil
	}
	if e.Operation() != nats.KeyValuePut {
		return "", nil
	}
	return string(e.Value()), nil
}

func (b *Bus) KVCas(ctx context.Context, bucket, key, value string, revision int64) (int64, error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return 0, nil
	}
	rev, err := kv.Update(key, []byte(value), uint64(revision))
	if err != nil {
		return 0, nil
	}
	return int64(rev), nil
}

func (b *Bus) KVDelete(ctx context.Context, bucket, key string) error {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return err
	}
	return kv.Delete(key)
}

// Replay returns the retained envelopes for a channel (JetStream stream
// contents, oldest first). Backs session-events replay.
func (b *Bus) Replay(ctx context.Context, ch string) ([]*easybus.Envelope, error) {
	out := []*easybus.Envelope{}
	// Find the stream covering this subject.
	for _, info := range []string{streamEvents, streamMailbox, streamDLQ} {
		si, err := b.js.StreamInfo(info)
		if err != nil {
			continue
		}
		subjects := si.Config.Subjects
		matched := false
		for _, s := range subjects {
			if s == ch || strings.ContainsAny(s, "*>") && subjectMatch(s, ch) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		sub, err := b.js.SubscribeSync(ch, nats.BindStream(info))
		if err != nil {
			return nil, err
		}
		defer sub.Unsubscribe()
		for {
			m, err := sub.NextMsg(500 * time.Millisecond)
			if err != nil {
				break
			}
			env, err := decode(m)
			if err != nil {
				continue
			}
			out = append(out, env)
		}
		break
	}
	return out, nil
}

func (b *Bus) Close() error {
	b.nc.Drain()
	b.nc.Close()
	return nil
}

// subjectMatch implements NATS-style `*` / `>` matching for stream subject
// filters.
func subjectMatch(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	pi := 0
	for si := 0; si < len(s); si++ {
		if pi >= len(p) {
			return false
		}
		pt := p[pi]
		if pt == ">" {
			return true
		}
		if pt == "*" {
			pi++
			continue
		}
		if pt != s[si] {
			return false
		}
		pi++
	}
	return pi == len(p)
}
