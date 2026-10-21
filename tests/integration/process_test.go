//go:build acceptance

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// This acceptance test starts actual OS processes, not an in-memory worker mock.
func TestThousandJobsThreeWorkerProcessesAndAPIRestart(t *testing.T) {
	s, dsn := database(t)
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "taskmesh.exe")
	build := exec.Command("go", "build", "-o", binary, "./cmd/taskmesh")
	build.Dir = filepath.Join("..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	start := func(mode, port, name string) *exec.Cmd {
		cmd := exec.Command(binary, mode)
		cmd.Env = append(os.Environ(), "DATABASE_URL="+dsn, "TASKMESH_API_KEY="+token, "PORT="+port, "WORKER_NAME="+name)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		return cmd
	}
	for i := 0; i < 3; i++ {
		start("worker", freePort(t), fmt.Sprintf("acceptance-%d", i))
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM workers").Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("workers did not register")
		}
		time.Sleep(50 * time.Millisecond)
	}
	port := freePort(t)
	server := start("api", port, "")
	base := "http://127.0.0.1:" + port
	awaitReady(t, base)
	client := &http.Client{Timeout: 10 * time.Second}
	began := time.Now()
	var wg sync.WaitGroup
	for producer := 0; producer < 10; producer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				req, err := http.NewRequest("POST", base+"/api/v1/jobs", strings.NewReader(`{"type":"report.generate","queue":"reports","payload":{"values":[10,20,30]}}`))
				if err != nil {
					t.Error(err)
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					t.Errorf("submit HTTP %d: %s", resp.StatusCode, body)
					return
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	deadline = time.Now().Add(90 * time.Second)
	for {
		var done int
		if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='SUCCEEDED'").Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == 1000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/1000 completed", done)
		}
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(began)
	var workers, attempts, badResults int
	if err := s.Pool.QueryRow(ctx, `SELECT count(DISTINCT worker_id),count(*) FROM job_attempts`).Scan(&workers, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE result->>'sum'<>'60' OR attempt_count<>1`).Scan(&badResults); err != nil {
		t.Fatal(err)
	}
	if workers != 3 || attempts != 1000 || badResults != 0 {
		t.Fatalf("workers=%d attempts=%d bad_results=%d", workers, attempts, badResults)
	}
	if err := server.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = server.Wait()
	newPort := freePort(t)
	start("api", newPort, "")
	awaitReady(t, "http://127.0.0.1:"+newPort)
	req, _ := http.NewRequest("GET", "http://127.0.0.1:"+newPort+"/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var jobs []map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || len(jobs) != 100 {
		t.Fatalf("restart read HTTP=%d jobs=%d", resp.StatusCode, len(jobs))
	}
	t.Logf("1000 durable report jobs, 3 worker processes, %s (%.1f jobs/s including submissions); API restart retained results", elapsed.Round(time.Millisecond), 1000/elapsed.Seconds())
}
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	return port
}
func awaitReady(t *testing.T, base string) {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("API not ready")
}
