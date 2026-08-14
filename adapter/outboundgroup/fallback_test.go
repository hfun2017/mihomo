package outboundgroup

import (
	"context"
	"sync"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

const fallbackTestURL = "invalid-test-url"

func TestFallbackSelectsRandomHealthyPrimary(t *testing.T) {
	fallback, _ := newFallbackForTest(t, "node-1", "node-2", "node-3")
	fallback.randomIntN = func(n int) int { return n - 1 }

	if got := fallback.Now(); got != "node-3" {
		t.Fatalf("random primary = %q, want %q", got, "node-3")
	}
}

func TestFallbackKeepsRandomPrimary(t *testing.T) {
	fallback, _ := newFallbackForTest(t, "node-1", "node-2", "node-3")

	selected := fallback.Now()
	for i := 0; i < 10; i++ {
		if got := fallback.Now(); got != selected {
			t.Fatalf("fallback changed healthy primary from %q to %q", selected, got)
		}
	}
}

func TestFallbackReplacesUnavailablePrimary(t *testing.T) {
	fallback, proxies := newFallbackForTest(t, "node-1", "node-2", "node-3")

	selected := fallback.Now()
	for _, proxy := range proxies {
		if proxy.Name() != selected {
			continue
		}
		if _, err := proxy.URLTest(context.Background(), fallbackTestURL, nil); err == nil {
			t.Fatal("test proxy unexpectedly passed an invalid URL test")
		}
		break
	}

	if got := fallback.Now(); got == selected {
		t.Fatalf("fallback kept unavailable primary %q", selected)
	}
}

func TestFallbackReplacesMissingPrimary(t *testing.T) {
	fallback, proxies := newFallbackForTest(t, "node-1", "node-2")
	fallback.ForceSet("removed-node")

	selected := fallback.Now()
	for _, proxy := range proxies {
		if proxy.Name() == selected {
			return
		}
	}
	t.Fatalf("fallback selected unknown proxy %q", selected)
}

func TestFallbackConcurrentSelection(t *testing.T) {
	fallback, _ := newFallbackForTest(t, "node-1", "node-2", "node-3")
	selected := fallback.Now()

	results := make(chan string, 20)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- fallback.Now()
		}()
	}
	wg.Wait()
	close(results)

	for got := range results {
		if got != selected {
			t.Fatalf("fallback changed healthy primary from %q to %q", selected, got)
		}
	}
}

func TestFallbackOnlyReportsManualSelectionAsFixed(t *testing.T) {
	fallback, _ := newFallbackForTest(t, "node-1", "node-2")
	_ = fallback.Now()
	if got := fallback.selectedName(); got != "" {
		t.Fatalf("random primary was reported as fixed: %q", got)
	}

	if err := fallback.Set("node-2"); err != nil {
		t.Fatalf("set fallback proxy: %v", err)
	}
	if got := fallback.selectedName(); got != "node-2" {
		t.Fatalf("fixed proxy = %q, want %q", got, "node-2")
	}
}

func newFallbackForTest(t *testing.T, names ...string) (*Fallback, []C.Proxy) {
	t.Helper()

	proxies := make([]C.Proxy, 0, len(names))
	for _, name := range names {
		proxies = append(proxies, adapter.NewProxy(outbound.NewRejectWithOption(outbound.RejectOption{Name: name})))
	}

	healthCheck := provider.NewHealthCheck(proxies, fallbackTestURL, 1, 0, false, nil)
	proxyProvider, err := provider.NewCompatibleProvider("test-provider", proxies, healthCheck)
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}
	t.Cleanup(func() { _ = proxyProvider.Close() })

	emptyFallback := adapter.NewProxy(outbound.NewCompatible())
	return NewFallback(&GroupCommonOption{
		Name: "test-fallback",
		URL:  fallbackTestURL,
	}, emptyFallback, []P.ProxyProvider{proxyProvider}), proxies
}
