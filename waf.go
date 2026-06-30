package main

import (
	"bufio"
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
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.1 Safari/605.1.15",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 Version/17.1 Mobile/15E148 Safari/604.1",
	"Mozilla/5.0 (Linux; Android 13; SM-S908E) AppleWebKit/537.36 Chrome/112.0.0.0 Mobile Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
}

type Result struct {
	Proxy string
	RPS float64
	P50, P95, P99 int64
	ErrorPct float64
	Skipped bool
}

func randUA() string {
	return userAgents[rand.Intn(len(userAgents))]
}

func fetchProxies(proxyURL string) ([]string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(proxyURL)
	if err!= nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode!= 200 {
		return nil, fmt.Errorf("proxy url status: %d", resp.StatusCode)
	}

	var proxies []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
	}
	proxies = append(proxies, line)
	}
	return proxies, scanner.Err()
}

func testProxy(target string, proxyStr string, concurrency int, reqCount int, maxErrPct float64) Result {
	proxyURL, err := url.Parse("http://" + proxyStr)
	if err!= nil {
		return Result{Proxy: proxyStr, ErrorPct: 100.0}
	}

	transport := &http.Transport{
	Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Banyak web CN pakai cert aneh
		MaxIdleConns: concurrency,
		MaxIdleConnsPerHost: concurrency,
	IdleConnTimeout: 30 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}

	var latencies []int64
	var success, failed int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	start := time.Now()
	for i := 0; i < reqCount; i++ {
	// Auto rotate: cek error rate di tengah jalan
		if i > 10 {
			currErr := float64(atomic.LoadInt64(&failed)) / float64(i) * 100.0
			if currErr > maxErrPct {
				fmt.Printf(" SKIP Err:%.1f%% ", currErr)
				return Result{Proxy: proxyStr, ErrorPct: currErr, Skipped: true}
			}
	}

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			req, _ := http.NewRequest("GET", target, nil)
			req.Header.Set("User-Agent", randUA())
			req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*")
			req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

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
	rps := float64(done) / total
	errPct := 100.0 * float64(failed) / float64(done)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 int64
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}
	return Result{Proxy: proxyStr, RPS: rps, P50: p50, P95: p95, P99: p99, ErrorPct: errPct}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	
	urlFlag := flag.String("url", "https://www.myweb.com", "Target URL web China")
	proxyURLFlag := flag.String("proxy-url", "", "URL list proxy, 1 per baris")
	proxyFile := flag.String("proxies", "proxies.txt", "Fallback file proxy")
	concurrency := flag.Int("max-c", 8, "Concurrency per proxy. Web CN kuat di 8-16")
	reqs := flag.Int("n", 100, "Requests per proxy")
	maxErr := flag.Float64("max-err", 50.0, "Auto rotate jika error > %")
	out := flag.String("out", "waf_cn.csv", "Output CSV")
	flag.Parse()

	var proxies []string
	var err error
	if *proxyURLFlag!= "" {
		fmt.Printf("[+] Download proxy dari: %s\n", *proxyURLFlag)
	proxies, err = fetchProxies(*proxyURLFlag)
	} else {
		fmt.Printf("[+] Baca proxy dari: %s\n", *proxyFile)
		file, err := os.Open(*proxyFile)
		if err!= nil {
			panic(err)
	}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			proxies = append(proxies, strings.TrimSpace(scanner.Text()))
	}
		file.Close()
	}
	if err!= nil {
		panic(err)
	}
	if len(proxies) == 0 {
		panic("proxy kosong")
	}

	fmt.Printf("[+] Test %d proxy -> %s | C=%d N=%d MaxErr=%.0f%%\n", len(proxies), *urlFlag, *concurrency, *reqs, *maxErr)
	var results []Result
	for idx, p := range proxies {
		fmt.Printf("[%d/%d] Proxy ***@%s ", idx+1, len(proxies), strings.Split(p, "@")[len(strings.Split(p, "@"))-1])
		res := testProxy(*urlFlag, p, *concurrency, *reqs, *maxErr)
		if res.Skipped {
			fmt.Printf("DI-SKIP\n")
	} else {
			fmt.Printf("DONE | RPS: %.2f | p99: %dms | Err: %.1f%%\n", res.RPS, res.P99, res.ErrorPct)
	}
		results = append(results, res)
		time.Sleep(200 * time.Millisecond) // jeda biar gak gaspol banget
	}

	f, _ := os.Create(*out)
	w := csv.NewWriter(f)
	w.Write([]string{"proxy", "rps", "p50_ms", "p95_ms", "p99_ms", "error_pct", "skipped"})
	for _, r := range results {
		w.Write([]string{r.Proxy, fmt.Sprintf("%.2f", r.RPS), fmt.Sprint(r.P50), fmt.Sprint(r.P95), fmt.Sprint(r.P99), fmt.Sprintf("%.1f", r.ErrorPct), fmt.Sprint(r.Skipped)})
	}
	w.Flush()
	fmt.Println("\nSelesai. Hasil:", *out)
}
