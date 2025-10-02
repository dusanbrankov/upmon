package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var urlSchemeRX = regexp.MustCompile("^https?$")

var ErrUnknownFormat = errors.New("-output: unknown format")

type urlList []string

type options struct {
	urls     urlList
	interval time.Duration
	output   string
	method   string
}

func (s *urlList) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *urlList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var opts options

	flag.Var(&opts.urls, "url", "URL to monitor (can be specified multiple times)")
	flag.DurationVar(&opts.interval, "interval", time.Minute, "Interval between checks, e.g. 30s, 1m, 2h")
	flag.StringVar(&opts.output, "output", "kv", "Output format: kv (key-value), json, pretty (pretty-printed JSON)")
	flag.StringVar(&opts.method, "method", "get", "HTTP method to use for requests: get, head")
	flag.Parse()

	if len(opts.urls) == 0 {
		errorAndExit("at least one URL must be provided with -url")
	}

	parsed, errs := parseURLs(opts.urls)
	if len(errs) > 0 {
		for _, err := range errs {
			printError("%s\n", err)
		}
		os.Exit(1)
	}

	errs = pingHosts(parsed)
	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
		os.Exit(1)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          20,
			MaxConnsPerHost:       2,
			IdleConnTimeout:       opts.interval + 10*time.Second,
			DisableCompression:    true,  // We don't need response body
			DisableKeepAlives:     false, // Enable keep-alives for better performance
			ResponseHeaderTimeout: 5 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
		},
	}

	results := make(chan result, len(opts.urls))
	method := strings.ToUpper(opts.method)

	// First run before the ticker
	for _, url := range opts.urls {
		go checkURL(client, url, method, results)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, url := range parsed {
				go checkURL(client, url.String(), method, results)
			}
		case res := <-results:
			if err := res.log(opts.output); err != nil {
				if errors.Is(err, ErrUnknownFormat) {
					fmt.Fprintf(os.Stderr, "upmon: %v\n", err)
				} else {
					fmt.Fprintf(os.Stderr, "upmon: error: %v\n", err)
				}
				os.Exit(1)
			}
		case <-quit:
			fmt.Println("upmon: shutting down...")
			return
		}
	}
}

func (r result) log(format string) error {
	var err error
	switch format {
	case "json", "pretty":
		err = r.logJSON(format == "pretty")
	case "kv":
		r.logKV()
	default:
		err = ErrUnknownFormat
	}
	return err
}

func (r result) logJSON(pretty bool) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(r)
}

func (r result) logKV() {
	fmt.Printf("time=%s url=%s status=%d latency=%s retries=%d error=%q\n",
		r.Time,
		r.URL,
		r.Status,
		r.Latency,
		r.Retries,
		r.ErrorMsg,
	)
}

type result struct {
	Time     string `json:"time"`
	URL      string `json:"url"`
	Status   int    `json:"status"`
	Latency  string `json:"latency"`
	Retries  int    `json:"retries"`
	Error    error  `json:"-"`
	ErrorMsg string `json:"error"`
}

func checkURL(client *http.Client, url string, method string, ch chan<- result) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		ch <- result{Time: timestamp(), URL: url, Error: err}
		return
	}
	req.Header.Set("User-Agent", "upmon-cli/0.1")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		ch <- result{Time: timestamp(), URL: url, ErrorMsg: err.Error()}
		return
	} else if !okResponse(resp.StatusCode) {
		ch <- result{Time: timestamp(), URL: url, Status: resp.StatusCode, ErrorMsg: http.StatusText(resp.StatusCode)}
		return
	}
	defer resp.Body.Close()

	ch <- result{
		Time:     timestamp(),
		URL:      url,
		Status:   resp.StatusCode,
		Latency:  time.Since(start).Round(time.Millisecond).String(),
		Retries:  0,
		ErrorMsg: "",
	}
}

func okResponse(status int) bool {
	return status >= 200 && status < 300
}

func parseURLs(urls []string) ([]*url.URL, []error) {
	var errs []error
	parsed := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		url, err := url.ParseRequestURI(u)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		scheme := url.Scheme
		if !urlSchemeRX.MatchString(scheme) {
			errs = append(errs, fmt.Errorf("protocol %q not supported: %s", url.Scheme, url.String()))
			continue
		}
		parsed = append(parsed, url)
	}
	return parsed, errs
}

func pingHosts(urls []*url.URL) []error {
	var errs []error

	resolver := net.Resolver{
		PreferGo:     true,
		StrictErrors: true,
	}

	for _, u := range urls {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := resolver.LookupHost(ctx, u.Hostname())
		cancel()
		if err != nil {
			var dnsErr *net.DNSError
			if errors.As(err, &dnsErr) && strings.Contains(err.Error(), "connection refused") {
				return []error{fmt.Errorf("network issue: %w", err)}
			}
			errs = append(errs, fmt.Errorf("check host %s: %w", u.Hostname(), err))
		}
	}

	return errs
}

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func printError(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "upmon: "+format, a...)
}

func errorAndExit(format string, a ...any) {
	printError(format, a...)
	os.Exit(1)
}

func fatalError(format string, a ...any) {
	printError("error: "+format, a...)
	os.Exit(1)
}
