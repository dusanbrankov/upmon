package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
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

type options struct {
	interval time.Duration
	quiet    bool
}

func main() {
	var opts options

	flag.DurationVar(&opts.interval, "interval", time.Minute, "Interval between checks, e.g. 30s, 1m, 2h")
	flag.BoolVar(&opts.quiet, "quiet", false, "Suppress output for successful lookups")
	flag.Parse()

	urls := []string{
		"https://wunderbaum.expert",
		"https://ic-berlin.club",
		"https://clonesintergalactic.com",
		"https://dbran.cc",
	}

	parsed, errs := parseURLs(urls)
	if len(errs) > 0 {
		for _, err := range errs {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
		fmt.Fprint(os.Stderr, "\nURLs must have the following format: http[s]://[<subdomain>.]example.com\n")
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
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:       20,
			MaxConnsPerHost:    2,
			IdleConnTimeout:    opts.interval + 10*time.Second,
			DisableCompression: true,  // We don't need response body
			DisableKeepAlives:  false, // Enable keep-alives for better performance
		},
	}

	results := make(chan result, len(urls))

	// First run before the ticker
	for _, url := range parsed {
		go checkURL(client, url.String(), results)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, url := range parsed {
				go checkURL(client, url.String(), results)
			}
		case res := <-results:
			logResult(res, opts)
		case <-quit:
			fmt.Println("upmon: shutting down...")
			return
		default:
			// non-blocking: if no results, loop continues
		}
	}
}

func logResult(res result, opts options) {
	if res.err != nil {
		log.Printf("%-40s %q\n", res.url, res.err.Error())
	} else if !okResponse(res.statusCode) {
		log.Printf("%-40s status code: %d\n", res.url, res.statusCode)
	} else if !opts.quiet {
		log.Printf("%-40s %s\n", res.url, res.latency)
	}
}

type result struct {
	url        string
	statusCode int
	err        error
	latency    time.Duration
}

func checkURL(client *http.Client, url string, ch chan<- result) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		ch <- result{url, 0, err, 0}
		return
	}
	req.Header.Set("User-Agent", "upmon-cli/0.1")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		ch <- result{url, 0, err, 0}
		return
	}
	defer resp.Body.Close()
	ch <- result{
		url:        url,
		statusCode: resp.StatusCode,
		err:        nil,
		latency:    time.Since(start).Round(time.Millisecond),
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
			errs = append(errs, fmt.Errorf("%s: unsupported scheme, must be \"http\" or \"https\"", url.Hostname()))
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
