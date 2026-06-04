// Handles the UserAgent and X-Xata-Agent http headers
// X-Xata-Agent is a header sent by the console, CLI, and "SDK" to identify themselves. Since we
// don't want to embed analytics directly. They send semicolon-separated key=value pairs:
//
//	client=@xata.io/api; version=0.1.0; service=console; session=phsid_abc  // console.xata.io
//	client=@xata.io/api; version=0.1.0; service=cli; cli_command_id=branch:list; cli_invocation_id=abc123; ci=github-actions; pr=true; ai_agent=cursor  // CLI
//	client=@xata.io/api; version=0.1.0                                          // end user using SDK directly
//
// The parsed values are stored in request context for use in tracing and analytics (PostHog).
package clienthttpheaders

import (
	"context"
	"strings"
)

type ctxKey struct{}

type ParsedXataAgent struct {
	Client          string
	Version         string
	Service         string
	CLICommandID    string
	CLIInvocationID string
	Session         string
	CI              string
	PR              string
	AIAgent         string
}

type ParsedHeaders struct {
	UserAgent string
	XataAgent ParsedXataAgent
}

func parseXataAgent(xataAgentHeader string) ParsedXataAgent {
	var result ParsedXataAgent
	for part := range strings.SplitSeq(xataAgentHeader, ";") {
		k, v, ok := parsePair(part)
		if !ok {
			continue
		}
		switch k {
		case "client":
			result.Client = v
		case "version":
			result.Version = v
		case "service":
			result.Service = v
		case "cli_command_id":
			result.CLICommandID = v
		case "cli_invocation_id":
			result.CLIInvocationID = v
		case "session":
			result.Session = v
		case "ci":
			result.CI = v
		case "pr":
			result.PR = v
		case "ai_agent":
			result.AIAgent = v
		}
	}
	return result
}

func parsePair(s string) (string, string, bool) {
	k, v, ok := strings.Cut(strings.TrimSpace(s), "=")
	k = strings.TrimSpace(k)
	if !ok || k == "" {
		return "", "", false
	}
	return k, strings.TrimSpace(v), true
}

func NewParsedHeaders(userAgent, xataAgent string) *ParsedHeaders {
	return &ParsedHeaders{
		UserAgent: userAgent,
		XataAgent: parseXataAgent(xataAgent),
	}
}

// NewContext returns a new context with the given ParsedHeaders stored in it.
func NewContext(ctx context.Context, h *ParsedHeaders) context.Context {
	return context.WithValue(ctx, ctxKey{}, h)
}

// FromContext retrieves the ParsedHeaders from the context.
func FromContext(ctx context.Context) *ParsedHeaders {
	h, _ := ctx.Value(ctxKey{}).(*ParsedHeaders)
	return h
}
