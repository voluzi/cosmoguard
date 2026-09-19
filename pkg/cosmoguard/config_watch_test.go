package cosmoguard

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gotest.tools/assert"
)

const configWatchTimeout = 3 * time.Second

type atomicWriterVolume struct {
	dir        string
	configPath string
	generation int
}

func newAtomicWriterVolume(t *testing.T, initial []byte) *atomicWriterVolume {
	t.Helper()

	volume := &atomicWriterVolume{
		dir:        t.TempDir(),
		generation: 1,
	}
	volume.configPath = filepath.Join(volume.dir, "cosmoguard.yaml")
	volume.publish(t, initial)
	assert.NilError(t, os.Symlink(filepath.Join("..data", "cosmoguard.yaml"), volume.configPath))
	return volume
}

func (v *atomicWriterVolume) publish(t *testing.T, config []byte) {
	t.Helper()

	generationName := fmt.Sprintf("..2026_09_19_00_00_%02d", v.generation)
	v.generation++
	generationDir := filepath.Join(v.dir, generationName)
	assert.NilError(t, os.Mkdir(generationDir, 0o755))
	assert.NilError(t, os.WriteFile(filepath.Join(generationDir, "cosmoguard.yaml"), config, 0o644))

	temporaryLink := filepath.Join(v.dir, "..data_tmp")
	assert.NilError(t, os.Symlink(generationName, temporaryLink))
	assert.NilError(t, os.Rename(temporaryLink, filepath.Join(v.dir, "..data")))
}

func waitForConfigWatcher(t *testing.T, cg *CosmoGuard, watchedDir string) {
	t.Helper()

	target := filepath.Clean(watchedDir)
	deadline := time.Now().Add(configWatchTimeout)
	for time.Now().Before(deadline) {
		watcher := cg.configWatcher.Load()
		if watcher != nil {
			for _, watched := range watcher.WatchList() {
				if filepath.Clean(watched) == target {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("config watcher did not begin watching %s", target)
}

func currentConfig(cg *CosmoGuard) *Config {
	cg.configMutex.Lock()
	defer cg.configMutex.Unlock()
	return cg.cfg
}

func currentReloadStatus(cg *CosmoGuard) ReloadStatus {
	return listReloadStatus(cg)["reload"].(ReloadStatus)
}

func assertNoReloadFor(t *testing.T, cg *CosmoGuard, previous ReloadStatus, duration time.Duration) {
	t.Helper()

	assertUnchanged := func() {
		status := currentReloadStatus(cg)
		if status.TimestampMs != previous.TimestampMs {
			t.Fatalf("unexpected config reload: before=%+v after=%+v", previous, status)
		}
	}
	assertUnchanged()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			assertUnchanged()
		case <-timer.C:
			assertUnchanged()
			return
		}
	}
}

func assertLCDStatus(t *testing.T, cg *CosmoGuard, want int) {
	t.Helper()

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "http://cosmoguard.test/policy", nil)
	cg.lcdProxy.ServeHTTP(response, request)
	assert.Equal(t, response.Code, want)
}

func waitForReloadAfter(t *testing.T, cg *CosmoGuard, timestampMs int64) ReloadStatus {
	t.Helper()

	deadline := time.Now().Add(configWatchTimeout)
	for time.Now().Before(deadline) {
		status := currentReloadStatus(cg)
		if status.TimestampMs > timestampMs {
			return status
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for a config reload after timestamp %d", timestampMs)
	return ReloadStatus{}
}

func watchConfigYAML(header, upstreamURL string, action RuleAction, path string) []byte {
	return []byte(header + fmt.Sprintf(`
nodes:
  - name: test-upstream
    lcdURL: %q
lcd:
  default: allow
  rules:
    - priority: 100
      action: %s
      paths: [%q]
      methods: [GET]
`, upstreamURL, action, path))
}

func TestWatchConfigFile_ReloadsAtomicWriterPublications(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Kubernetes AtomicWriter publication uses Linux inotify semantics")
	}

	header := portYAMLHeader(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	volume := newAtomicWriterVolume(t, watchConfigYAML(header, upstream.URL, RuleActionAllow, "/policy"))

	cg, err := NewFromFile(volume.configPath)
	assert.NilError(t, err)
	t.Cleanup(func() { _ = cg.Shutdown(t.Context()) })
	cg.applyRules()

	go func() { _ = cg.WatchConfigFile() }()
	waitForConfigWatcher(t, cg, volume.dir)

	originalCfg := currentConfig(cg)
	for i := 0; i < 5; i++ {
		noise := filepath.Join(volume.dir, fmt.Sprintf("..data-noise-%d", i))
		assert.NilError(t, os.WriteFile(noise, []byte("noise"), 0o644))
		assert.NilError(t, os.Remove(noise))
	}
	assertNoReloadFor(t, cg, ReloadStatus{}, configWatchTimeout)
	assert.Equal(t, currentConfig(cg), originalCfg, "unrelated directory churn must not reload config")
	assertLCDStatus(t, cg, http.StatusNoContent)

	previousReload := currentReloadStatus(cg)
	assert.NilError(t, os.Remove(filepath.Join(volume.dir, "..data")))
	assertNoReloadFor(t, cg, previousReload, configWatchTimeout)
	assert.Equal(t, currentConfig(cg), originalCfg, "removing ..data must not reload config")
	assertLCDStatus(t, cg, http.StatusNoContent)

	volume.publish(t, watchConfigYAML(header, upstream.URL, RuleActionDeny, "/policy"))
	deniedReload := waitForReloadAfter(t, cg, previousReload.TimestampMs)
	assert.Equal(t, deniedReload.Success, true)
	assertLCDStatus(t, cg, http.StatusUnauthorized)
	lastGood := currentConfig(cg)

	volume.publish(t, watchConfigYAML(header, upstream.URL, RuleActionAllow, "[invalid-glob"))
	failedReload := waitForReloadAfter(t, cg, deniedReload.TimestampMs)
	assert.Equal(t, failedReload.Success, false)
	assert.Equal(t, currentConfig(cg), lastGood, "malformed publication must preserve the last-known-good config")
	assertLCDStatus(t, cg, http.StatusUnauthorized)

	reloads := []struct {
		action RuleAction
		status int
	}{
		{action: RuleActionAllow, status: http.StatusNoContent},
		{action: RuleActionDeny, status: http.StatusUnauthorized},
		{action: RuleActionAllow, status: http.StatusNoContent},
	}
	lastReload := failedReload
	for _, reload := range reloads {
		volume.publish(t, watchConfigYAML(header, upstream.URL, reload.action, "/policy"))
		lastReload = waitForReloadAfter(t, cg, lastReload.TimestampMs)
		assert.Equal(t, lastReload.Success, true)
		assertLCDStatus(t, cg, reload.status)
	}
}
