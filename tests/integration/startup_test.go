//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBatch0_MissingDependencyConfigFailsStartup(t *testing.T) {
	bin := buildNotifier(t)
	root := repoRoot(t)
	const secret = "supersecret-s00"

	out, code := runNotifier(t, root, bin, []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"AUTH_CONFIG_REF=",
	})
	if code == 0 || !strings.Contains(out, "AUTH_CONFIG_REF") || strings.Contains(out, secret) {
		t.Fatalf("empty AUTH_CONFIG_REF exit=%d output=%s", code, out)
	}

	out, code = runNotifier(t, root, bin, []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"AUTH_CONFIG_REF=" + filepath.Join(root, "config", "missing-identities.json"),
	})
	if code == 0 || !strings.Contains(out, "AUTH_CONFIG_REF") {
		t.Fatalf("missing identity file exit=%d output=%s", code, out)
	}

	out, code = runNotifier(t, root, bin, []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"AUTH_CONFIG_REF=" + filepath.Join(root, "config", "identities.example.json"),
		"MYSQL_DSN=user:" + secret + "@tcp(127.0.0.1:1)/notifier?parseTime=true",
		"REDIS_ADDR=127.0.0.1:6379",
		"ROCKETMQ_ENDPOINT=127.0.0.1:8081",
	})
	if code == 0 || !strings.Contains(out, "MYSQL_DSN") || strings.Contains(out, secret) {
		t.Fatalf("bad mysql exit=%d output=%s", code, out)
	}
}

func TestBatch0_ReadyzReflectsDependencies(t *testing.T) {
	bin := buildNotifier(t)
	root := repoRoot(t)
	cmd := exec.Command(bin)
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"HTTP_ADDR=127.0.0.1:18080",
		"AUTH_CONFIG_REF=" + filepath.Join(root, "config", "identities.example.json"),
		"MYSQL_DSN=" + gooseDSN("notifier"),
		"REDIS_ADDR=" + mysqlEnv("REDIS_ADDR", "127.0.0.1:6379"),
		"ROCKETMQ_ENDPOINT=" + mysqlEnv("ROCKETMQ_ENDPOINT", "127.0.0.1:8081"),
		"HTTP_SHUTDOWN_TIMEOUT=5s",
	}
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_, _ = cmd.Process.Wait()
		}
	})

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(20 * time.Second)
	var ready *http.Response
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://127.0.0.1:18080/readyz")
		if err == nil {
			ready = resp
			if resp.StatusCode == http.StatusOK {
				break
			}
			resp.Body.Close()
			ready = nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	if ready == nil || ready.StatusCode != http.StatusOK {
		body := ""
		if ready != nil {
			raw, _ := io.ReadAll(ready.Body)
			ready.Body.Close()
			body = string(raw)
		}
		t.Fatalf("readyz did not become ready: %s\n%s", body, output.String())
	}
	raw, _ := io.ReadAll(ready.Body)
	ready.Body.Close()
	if !strings.Contains(string(raw), `"ready"`) {
		t.Fatalf("ready body = %s", raw)
	}

	health, err := client.Get("http://127.0.0.1:18080/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", health.StatusCode)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown exit = %v\n%s", err, output.String())
		}
	case <-time.After(8 * time.Second):
		t.Fatal("process did not exit within HTTP_SHUTDOWN_TIMEOUT")
	}
}

func buildNotifier(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "notifier-s00")
	cmd := exec.Command("go", "build", "-o", path, "./cmd/notifier")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build notifier: %v\n%s", err, out)
	}
	return path
}

func runNotifier(t *testing.T, root, bin string, env []string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Dir = root
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	return string(out), code
}
