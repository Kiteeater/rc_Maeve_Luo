package provider

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"notifier/internal/conf"
	"notifier/internal/data_interface"
	"notifier/internal/domain"
)

// errTargetForbidden 由本服务拨号器在建立连接前给出，可证明请求未发出。
var errTargetForbidden = errors.New("provider: target address not allowed")

type cidrKey struct{}

// SecretSource 把不透明凭证句柄解析为本次请求使用的秘密；秘密只在内存中短暂存在。
type SecretSource interface {
	Secret(handle string) (string, error)
}

// Transport 实现 data_interface.HTTPCaller：不跟随重定向、不隐式重试、有界读取、拨号时校验实际目标地址。
type Transport struct {
	client   *http.Client
	secrets  SecretSource
	maxBytes int64
	now      func() time.Time
}

// NewTransport 按 S05 参数构造受控 HTTP 客户端。
func NewTransport(c conf.Provider, secrets SecretSource) *Transport {
	dialer := &net.Dialer{Timeout: c.DialTimeout}
	tr := &http.Transport{
		Proxy:                 nil,
		TLSHandshakeTimeout:   c.TLSTimeout,
		ResponseHeaderTimeout: c.HeaderTimeout,
		MaxIdleConns:          c.MaxIdleConnections,
		MaxIdleConnsPerHost:   c.MaxIdleConnections,
		IdleConnTimeout:       c.IdleTimeout,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			cidrs, _ := ctx.Value(cidrKey{}).([]string)
			for _, ip := range ips {
				if ipAllowed(ip.IP, cidrs) {
					return dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
				}
			}
			return nil, errTargetForbidden
		},
	}
	return &Transport{
		client: &http.Client{
			Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		secrets: secrets, maxBytes: int64(c.MaxResponseBytes), now: time.Now,
	}
}

func ipAllowed(ip net.IP, cidrs []string) bool {
	for _, c := range cidrs {
		if _, n, err := net.ParseCIDR(c); err == nil && n.Contains(ip) {
			return true
		}
	}
	return false
}

// Do 只发送一次。鉴权头与签名每次请求重新生成。
func (t *Transport) Do(ctx context.Context, in data_interface.BuiltRequest) domain.TransportFacts {
	u, err := url.Parse(in.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return domain.TransportFacts{TransportErr: "invalid target"}
	}
	req, err := http.NewRequestWithContext(context.WithValue(ctx, cidrKey{}, in.AllowedCIDRs), in.Method, u.String(), bytes.NewReader(in.Body))
	if err != nil {
		return domain.TransportFacts{TransportErr: "invalid request"}
	}
	// Idempotency-Key 会让 net/http 认为请求可重放；去掉 GetBody 禁止连接复用失败时的隐式重发。
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", in.IdempotencyKey)
	if err := t.sign(req, in); err != nil {
		return domain.TransportFacts{TransportErr: "credential unavailable"}
	}
	resp, err := t.client.Do(req)
	if err != nil {
		if errors.Is(err, errTargetForbidden) {
			return domain.TransportFacts{TransportErr: "target not allowed"}
		}
		return domain.TransportFacts{Invoked: true, TransportErr: classify(err), ReceivedAt: t.now().UTC()}
	}
	defer resp.Body.Close()
	f := domain.TransportFacts{Invoked: true, Status: resp.StatusCode, ReceivedAt: t.now().UTC(), RetryAfter: resp.Header.Get("Retry-After")}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		f.Redirect = true
	}
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, t.maxBytes+1))
	f.BytesRead = int64(len(body))
	switch {
	case int64(len(body)) > t.maxBytes:
		f.TooLarge = true
		body = body[:t.maxBytes]
	case rerr != nil:
		f.TransportErr = classify(rerr)
	default:
		f.ReadComplete = true
	}
	f.Body = body
	return f
}

func (t *Transport) sign(req *http.Request, in data_interface.BuiltRequest) error {
	if in.AuthType == "" || in.AuthType == "none" {
		return nil
	}
	secret, err := t.secrets.Secret(in.CredentialHandle)
	if err != nil {
		return err
	}
	switch in.AuthType {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+secret)
	case "hmac_sha256":
		ts := strconv.FormatInt(t.now().UTC().Unix(), 10)
		req.Header.Set("X-Signature-Time", ts)
		req.Header.Set("X-Signature", Signature(secret, ts, in.Method, req.URL.Path, in.Body))
	default:
		return errors.New("provider: unsupported auth type")
	}
	return nil
}

// Signature 计算 mock-json@1 HMAC：timestamp、method、path、body_sha256 以单个 LF 连接。
func Signature(secret, ts, method, path string, body []byte) string {
	sum := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "\n" + method + "\n" + path + "\n" + hex.EncodeToString(sum[:])))
	return hex.EncodeToString(mac.Sum(nil))
}

// classify 只给出粗粒度类别，不把错误原文（可能含地址）写入证据。
func classify(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "connection closed"
	default:
		return "connection error"
	}
}
