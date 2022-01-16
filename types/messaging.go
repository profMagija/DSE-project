package types

import (
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------
// ChatMessage

// NewEmpty implements types.Message.
func (c ChatMessage) NewEmpty() Message {
	return &ChatMessage{}
}

// Name implements types.Message.
func (ChatMessage) Name() string {
	return "chat"
}

// String implements types.Message.
func (c ChatMessage) String() string {
	return fmt.Sprintf("<%s>", c.Message)
}

// HTML implements types.Message.
func (c ChatMessage) HTML() string {
	return c.String()
}

// -----------------------------------------------------------------------------
// RumorsMessage

// NewEmpty implements types.Message.
func (r RumorsMessage) NewEmpty() Message {
	return &RumorsMessage{}
}

// Name implements types.Message.
func (RumorsMessage) Name() string {
	return "rumor"
}

// String implements types.Message.
func (r RumorsMessage) String() string {
	out := new(strings.Builder)
	out.WriteString("rumor{")
	for _, r := range r.Rumors {
		fmt.Fprint(out, r.String())
	}
	out.WriteString("}")
	return out.String()
}

// HTML implements types.Message.
func (r RumorsMessage) HTML() string {
	out := make([]string, len(r.Rumors))
	for i, r := range r.Rumors {
		out[i] = r.String()
	}

	return strings.Join(out, "<br/>")
}

// String implements types.Message.
func (r Rumor) String() string {
	return fmt.Sprintf("{%s-%d-%s}", r.Origin, r.Sequence, r.Msg.Type)
}

// -----------------------------------------------------------------------------
// AckMessage

// NewEmpty implements types.Message.
func (AckMessage) NewEmpty() Message {
	return &AckMessage{}
}

// Name implements types.Message
func (a AckMessage) Name() string {
	return "ack"
}

// String implements types.Message.
func (a AckMessage) String() string {
	return fmt.Sprintf("{ack for packet %s}", a.AckedPacketID)
}

// HTML implements types.Message.
func (a AckMessage) HTML() string {
	return fmt.Sprintf("ack for packet<br/>%s", a.AckedPacketID)
}

// -----------------------------------------------------------------------------
// StatusMessage

// NewEmpty implements types.Message.
func (StatusMessage) NewEmpty() Message {
	return &StatusMessage{}
}

// Name implements types.Message
func (s StatusMessage) Name() string {
	return "status"
}

// String implements types.Message.
func (s StatusMessage) String() string {
	out := new(strings.Builder)

	if len(s) > 5 {
		fmt.Fprintf(out, "{%d elements}", len(s))
	} else {
		for addr, seq := range s {
			fmt.Fprintf(out, "{%s-%d}", addr, seq)
		}
	}

	res := out.String()
	if res == "" {
		res = "{}"
	}

	return res
}

// HTML implements types.Message.
func (s StatusMessage) HTML() string {
	out := new(strings.Builder)

	for addr, seq := range s {
		fmt.Fprintf(out, "{%s-%d}", addr, seq)
	}

	res := out.String()
	if res == "" {
		res = "{}"
	}

	return res
}

// -----------------------------------------------------------------------------
// EmptyMessage

// NewEmpty implements types.Message.
func (EmptyMessage) NewEmpty() Message {
	return &EmptyMessage{}
}

// Name implements types.Message.
func (e EmptyMessage) Name() string {
	return "empty"
}

// String implements types.Message.
func (e EmptyMessage) String() string {
	return "{∅}"
}

// HTML implements types.Message.
func (e EmptyMessage) HTML() string {
	return "{∅}"
}

// -----------------------------------------------------------------------------
// PrivateMessage

// NewEmpty implements types.Message.
func (p PrivateMessage) NewEmpty() Message {
	return &PrivateMessage{}
}

// Name implements types.Message.
func (p PrivateMessage) Name() string {
	return "private"
}

// String implements types.Message.
func (p PrivateMessage) String() string {
	return fmt.Sprintf("private message for %s", p.Recipients)
}

// HTML implements types.Message.
func (p PrivateMessage) HTML() string {
	return fmt.Sprintf("private message for %s", p.Recipients)
}

// -----------------------------------------------------------------------------
// SplitEdgeMessage

// SplitEdgeMessage implements types.Message.
func (p SplitEdgeMessage) NewEmpty() Message {
	return &SplitEdgeMessage{}
}

// Name implements types.Message.
func (p SplitEdgeMessage) Name() string {
	return "SplitEdgeMessage"
}

// String implements types.Message.
func (p SplitEdgeMessage) String() string {
	return fmt.Sprintf("split edge message %v", p.TTL)
}

// HTML implements types.Message.
func (p SplitEdgeMessage) HTML() string {
	return fmt.Sprintf("split edge message %v", p.TTL)
}

// -----------------------------------------------------------------------------
// RedirectMessage

// RedirectMessage implements types.Message.
func (p RedirectMessage) NewEmpty() Message {
	return &RedirectMessage{}
}

// Name implements types.Message.
func (p RedirectMessage) Name() string {
	return "RedirectMessage"
}

// String implements types.Message.
func (p RedirectMessage) String() string {
	return fmt.Sprintf("redirect message splited by %s", p.Split)
}

// HTML implements types.Message.
func (p RedirectMessage) HTML() string {
	return fmt.Sprintf("redirect message splited by %s", p.Split)
}

// -----------------------------------------------------------------------------
// ConnectionHelloMessage

// ConnectionHelloMessage implements types.Message.
func (p ConnectionHelloMessage) NewEmpty() Message {
	return &ConnectionHelloMessage{}
}

// Name implements types.Message.
func (p ConnectionHelloMessage) Name() string {
	return "ConnectionHelloMessage"
}

// String implements types.Message.
func (p ConnectionHelloMessage) String() string {
	return "connection hello message"
}

// HTML implements types.Message.
func (p ConnectionHelloMessage) HTML() string {
	return "connection hello message"
}

// -----------------------------------------------------------------------------
// ConnectionNopeMessage

// ConnectionNopeMessage implements types.Message.
func (p ConnectionNopeMessage) NewEmpty() Message {
	return &ConnectionNopeMessage{}
}

// Name implements types.Message.
func (p ConnectionNopeMessage) Name() string {
	return "ConnectionNopeMessage"
}

// String implements types.Message.
func (p ConnectionNopeMessage) String() string {
	return "connection nope message"
}

// HTML implements types.Message.
func (p ConnectionNopeMessage) HTML() string {
	return "connection nope message"
}

// -----------------------------------------------------------------------------
// utility functions

// RumorByOrigin sorts rumor by origin
type RumorByOrigin []Rumor

func (r RumorByOrigin) Len() int {
	return len(r)
}

func (r RumorByOrigin) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RumorByOrigin) Less(i, j int) bool {
	return r[i].Origin < r[j].Origin
}

// ChatByMessage sorts chat message by their message
type ChatByMessage []*ChatMessage

func (c ChatByMessage) Len() int {
	return len(c)
}

func (c ChatByMessage) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ChatByMessage) Less(i, j int) bool {
	return c[i].Message < c[j].Message
}
