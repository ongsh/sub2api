package handler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Route both API-key and OAuth requests to a local HTTP server. No test
// credentials or requests may leave the process's loopback test endpoint.
type messagesEffortHTTPUpstream struct {
	client *http.Client
	target *url.URL
}

func (u *messagesEffortHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	local := req.Clone(req.Context())
	local.URL.Scheme = u.target.Scheme
	local.URL.Host = u.target.Host
	local.Host = u.target.Host
	return u.client.Do(local)
}

func (u *messagesEffortHTTPUpstream) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, accountID, concurrency)
}

func TestMessagesDefaultReasoningEffortHTTPBridge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, accountType := range []string{service.AccountTypeAPIKey, service.AccountTypeOAuth} {
		for _, stream := range []bool{false, true} {
			for _, tc := range []struct {
				name, model, fields, want string
				mapping                   service.ReasoningEffortMapping
				status                    int
				noCeiling                 bool
			}{
				{name: "disabled mapped", fields: `,"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, want: "low"},
				{name: "disabled no mapping", fields: `,"thinking":{"type":"disabled"}`, status: http.StatusBadRequest},
				{name: "disabled denied", fields: `,"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "deny"}, status: http.StatusForbidden},
				{name: "mapped minimal rejected", fields: `,"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "minimal"}, status: http.StatusBadRequest},
				{name: "mapping model mismatch", fields: `,"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low", Model: "other", MatchType: "exact"}, status: http.StatusBadRequest},
				{name: "medium default uncapped", mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, want: "medium"},
				{name: "ceiling alone leaves medium", want: "medium"},
				{name: "explicit low", fields: `,"output_config":{"effort":"low"}`, want: "low"},
				{name: "explicit medium capped", fields: `,"output_config":{"effort":"medium"}`, want: "low"},
				{name: "explicit high capped", fields: `,"output_config":{"effort":"high"}`, want: "low"},
				{name: "explicit xhigh capped", fields: `,"output_config":{"effort":"xhigh"}`, want: "low"},
				{name: "explicit max capped", fields: `,"output_config":{"effort":"max"}`, want: "low"},
				{name: "explicit none unchanged", fields: `,"output_config":{"effort":"none"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusBadRequest},
				{name: "explicit minimal unchanged", fields: `,"output_config":{"effort":"minimal"}`, status: http.StatusBadRequest},
				{name: "explicit low disabled mapped", fields: `,"output_config":{"effort":"low"},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, want: "low"},
				{name: "desktop explicit high disabled mapped", fields: `,"output_config":{"effort":"high"},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, noCeiling: true, want: "low"},
				{name: "desktop primary high adaptive preserved", fields: `,"output_config":{"effort":"high"},"thinking":{"type":"adaptive"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, noCeiling: true, want: "high"},
				{name: "explicit high disabled no mapping", fields: `,"output_config":{"effort":"high"},"thinking":{"type":"disabled"}`, status: http.StatusBadRequest},
				{name: "explicit high disabled denied", fields: `,"output_config":{"effort":"high"},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "deny"}, status: http.StatusForbidden},
				{name: "explicit high disabled mismatched model", fields: `,"output_config":{"effort":"high"},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low", Model: "other", MatchType: "exact"}, status: http.StatusBadRequest},
				{name: "null unchanged", fields: `,"output_config":{"effort":null},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusBadRequest},
				{name: "blank unchanged", fields: `,"output_config":{"effort":" "},"thinking":{"type":"disabled"}`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusBadRequest},
				{name: "raw alternate effort rejected", fields: `,"reasoning_effort":"none"`, mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusBadRequest},
				{name: "suffix none rejected", model: "gpt-6.1-sol-none", mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusBadRequest},
				// These aliases are rejected by the strict converter, which leaves
				// writing the error response to the outer handler.
				{name: "suffix whitespace rejected", model: "gpt-6.1-sol-none ", mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusOK},
				{name: "suffix underscores rejected", model: "gpt_6.1_sol_none", mapping: service.ReasoningEffortMapping{From: "none", To: "low"}, status: http.StatusOK},
			} {
				t.Run(fmt.Sprintf("%s/stream=%v/%s", accountType, stream, tc.name), func(t *testing.T) {
					requests := make(chan []byte, 1)
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}
						requests <- body
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_effort_test\",\"model\":\"gpt-6.1-sol\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
					}))
					defer upstream.Close()
					target, err := url.Parse(upstream.URL)
					require.NoError(t, err)
					client := upstream.Client()
					client.Timeout = 5 * time.Second
					client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
					cfg := &config.Config{}
					cfg.Security.URLAllowlist.AllowInsecureHTTP = true
					gateway := service.NewOpenAIGatewayService(
						nil, nil, nil, nil, nil, nil, nil, cfg, nil, nil, nil,
						nil, nil, &messagesEffortHTTPUpstream{client: client, target: target}, nil, nil, nil, nil, nil, nil, nil, nil,
					)
					account := &service.Account{
						ID: 1, Platform: service.PlatformOpenAI, Type: accountType, Concurrency: 1,
						Credentials: map[string]any{"api_key": "test-key", "access_token": "test-token", "base_url": upstream.URL},
					}
					group := &service.Group{Platform: service.PlatformOpenAI, MaxReasoningEffort: "low"}
					if tc.noCeiling {
						group.MaxReasoningEffort = ""
					}
					if tc.mapping.From != "" {
						group.ReasoningEffortMappings = []service.ReasoningEffortMapping{tc.mapping}
					}
					// Disabled thinking is the concrete bridge input that generates none;
					// omitted effort alone normally generates medium.
					model := tc.model
					if model == "" {
						model = "gpt-6.1-sol"
					}
					body := []byte(fmt.Sprintf(`{"model":%q,"max_tokens":32,"messages":[{"role":"user","content":"test"}],"stream":%v%s}`, model, stream, tc.fields))
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
					bindOpenAIReasoningEffortPolicyForMessagesRequest(c, &service.APIKey{Group: group}, body)
					result, err := gateway.ForwardAsAnthropic(c.Request.Context(), c, account, body, "", "")
					if tc.status != 0 {
						require.Error(t, err)
						require.Equal(t, tc.status, recorder.Code)
						require.Empty(t, requests, "invalid default must not reach the upstream")
						return
					}
					require.NoError(t, err)
					require.NotNil(t, result)
					select {
					case sent := <-requests:
						require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(sent, "model").String())
						require.Equal(t, tc.want, gjson.GetBytes(sent, "reasoning.effort").String())
					default:
						t.Fatal("bridge did not reach the HTTP mock")
					}
				})
			}
		}
	}
}
