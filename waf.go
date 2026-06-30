package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/proxy"
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

// 1. LIST PROXY SERVER/UPSTREAM
var upstreamServers = []string{
	"socks5://user1:pass1@1.1.1.1:1080",
}

const goodProxyFile = "proxy_jagoan.txt"

func randUA() string { return userAgents[rand.Intn(len(userAgents))] }

func getDialerFromURL(proxyURLStr string) (proxy.Dialer, error) {
	u, _ := url.Parse(proxyURLStr)
	var auth *proxy.Auth
	if u.User!= nil {
		pass, _ := u.User.Password()
		auth = &proxy.Auth{User: u.User.Username(), Password: pass}
	}
	switch u.Scheme {
	case "socks5":
		return proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
	case "http", "https":
		return proxy.FromURL(u, proxy.Direct)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}

type Result struct {
	Upstream, Target string
	RPS float64
	P50, P95, P99 int64
	ErrorPct float64
	Skipped bool
}

func testChain(target string, targetProxyStr string, upstreamProxyStr string, concurrency int, reqCount int, maxErrPct float64) Result {
	dialer, _ := getDialerFromURL(upstreamProxyStr)
	targetURL, _ := url.Parse("http://" + targetProxyStr)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) { return dialer.Dial(network, addr) },
	Proxy: http.ProxyURL(targetURL),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns: concurrency,
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}

	var latencies []int64
	var success, failed int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	start := time.Now()
	for i := 0; i < reqCount; i++ {
		if i > 5 && float64(atomic.LoadInt64(&failed))/float64(i)*100.0 > maxErrPct {
			return Result{Upstream: upstreamProxyStr, Target: targetProxyStr, ErrorPct: 100.0, Skipped: true}
	}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			req, _ := http.NewRequest("GET", target, nil)
			req.Header.Set("User-Agent", randUA())
			t0 := time.Now()
			resp, err := client.Do(req)
			dt := time.Since(t0).Milliseconds()
			if err == nil && resp.StatusCode == 200 {
				atomic.AddInt64(&success, 1)
				mu.Lock()
				latencies = append(latencies, dt)
				mu.Unlock()
			} else {
				atomic.AddInt64(&failed, 1)
			}
			if resp!= nil {
				resp.Body.Close()
			}
	}()
	}
	wg.Wait()
	total := time.Since(start).Seconds()
	done := int(success + failed)
	if done == 0 {
		return Result{Upstream: upstreamProxyStr, Target: targetProxyStr, ErrorPct: 100.0}
	}
	rps := float64(done) / total
	errPct := 100.0 * float64(failed) / float64(done)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 int64
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}
	return Result{Upstream: upstreamProxyStr, Target: targetProxyStr, RPS: rps, P50: p50, P95: p95, P99: p99, ErrorPct: errPct}
}

func shortProxy(s string) string { parts := strings.Split(s, "@"); return parts[len(parts)-1] }

func readProxiesFromStdin() []string {
	fmt.Println("[+] Paste list proxy target. Enter kosong 2x buat selesai")
	scanner := bufio.NewScanner(os.Stdin)
	var proxies []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" { break }
	proxies = append(proxies, line)
	}
	return proxies
}

func loadGoodProxies() []string {
	file, err := os.Open(goodProxyFile)
	if err!= nil {
		return nil
	}
	defer file.Close()
	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line!= "" {
			proxies = append(proxies, line)
	}
	}
	return proxies
}

func saveGoodProxies(proxies []string) {
	f, _ := os.Create(goodProxyFile)
	defer f.Close()
	for _, p := range proxies {
		f.WriteString(p + "\n")
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	
	urlFlag := flag.String("url", "https://www.myweb.com", "Target URL")
	useGood := flag.Bool("use-good", false, "Pake proxy dari proxy_jagoan.txt aja")
	clean := flag.Bool("clean", false, "Hapus proxy mati dari proxy_jagoan.txt")
	concurrency := flag.Int("max-c", 4, "Concurrency")
	reqs := flag.Int("n", 20, "Requests per chain. Mode clean pake 10 aja")
	maxErr := flag.Float64("max-err", 10.0, "Max error % buat disimpen")
	p99Max := flag.Int64("p99-max", 2000, "Max p99 ms buat disimpen")
	out := flag.String("out", "waf_full_chain.csv", "Output CSV")
	flag.Parse()

	if len(upstreamServers) == 0 {
		panic("isi upstreamServers di kode dulu")
	}

	var targetProxies []string
	if *useGood || *clean {
		fmt.Println("[+] Mode: Load dari file")
		targetProxies = loadGoodProxies()
		if len(targetProxies) == 0 {
			panic("proxy_jagoan.txt kosong")
	}
	} else {
		fmt.Println("[+] Mode: Scan Baru")
		targetProxies = readProxiesFromStdin()
	}

	fmt.Printf("\n[+] Upstream:%d Target:%d\n", len(upstreamServers), len(targetProxies))
	var results []Result
	var aliveProxies []string // 2. BUAT NYIMPEN YANG MASIH HIDUP

	for _, upstream := range upstreamServers {
		fmt.Printf("\n[UPSTREAM] ===> %s <===\n", shortProxy(upstream))
		for _, target := range targetProxies {
			res := testChain(*urlFlag, target, upstream, *concurrency, *reqs, *maxErr)
			fmt.Printf("Test %s via %s | RPS: %.2f | p99: %dms | Err: %.1f%%\n", shortProxy(target), shortProxy(upstream), res.RPS, res.P99, res.ErrorPct)
			results = append(results, res)

			// 3. LOGIKA SAVE
			if!*useGood &&!*clean && res.ErrorPct <= *maxErr && res.P99 <= *p99Max {
				aliveProxies = append(aliveProxies, target)
			}
			// 4. LOGIKA CLEAN: KUMPULIN YANG MASIH HIDUP AJA
			if *clean && res.ErrorPct < 100 {
				aliveProxies = append(aliveProxies, target)
			}
			time.Sleep(150 * time.Millisecond)
	}
	}

	// 5. OVERWRITE FILE DENGAN YANG HIDUP DOANG
	if *clean {
		fmt.Printf("\n[+] Cleaning... %d -> %d proxy hidup\n", len(targetProxies), len(aliveProxies))
		saveGoodProxies(aliveProxies)
	} else if!*useGood {
		fmt.Printf("\n[+] Saved %d new good proxies\n", len(aliveProxies))
		saveGoodProxies(aliveProxies)
	}

	f, _ := os.Create(*out)
	w := csv.NewWriter(f)
	w.Write([]string{"upstream", "target_proxy", "rps", "p50_ms", "p95_ms", "p99_ms", "error_pct"})
	for _, r := range results {
		w.Write([]string{r.Upstream, r.Target, fmt.Sprintf("%.2f", r.RPS), fmt.Sprint(r.P50), fmt.Sprint(r.P95), fmt.Sprint(r.P99), fmt.Sprintf("%.1f", r.ErrorPct)})
	}
	w.Flush()
	fmt.Println("Selesai. Hasil:", *out)
}
