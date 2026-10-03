package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const (
	version = "0.0.1"

	urlSchemeRgx = `^https?$`
)

var ErrUnknownFormat = errors.New("-output: unknown format")

var usage = `Usage: upmon [option]... <url>...
Monitor the response status of URLs at regular intervals.

Example:
  upmon -o json https://example.com https://httpbin.org/status/404

Options:
  -m    HTTP method to use when requesting URLs: GET, HEAD
        (default: GET)
  -i    Interval between checks: e.g. 30s, 1m, 2h
        (default: 1m)
  -o    Output format: text, json, json-pretty
        (default: text)
  -q	Suppress non-error messages
  -h    Show this help message
`

type urlList []string

type options struct {
	urls     urlList
	interval time.Duration
	output   string
	method   string
	quiet    bool
}

func main() {
	var opts options

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
	}

	flag.DurationVar(&opts.interval, "i", time.Minute, "")
	flag.StringVar(&opts.output, "o", "text", "")
	flag.StringVar(&opts.method, "m", http.MethodGet, "")
	flag.BoolVar(&opts.quiet, "q", false, "")
	flag.Parse()

	for _, arg := range flag.Args() {
		opts.urls = append(opts.urls, arg)
	}

	if len(opts.urls) == 0 {
		usageAndExit("at least one URL must be provided\n")
	}

	if err := validateInterval(opts.interval); err != nil {
		errorAndExit("%v\n", err)
	}

	if err := validateOutputFormat(opts.output); err != nil {
		errorAndExit("%v\n", err)
	}

	opts.method = strings.ToUpper(opts.method)

	if err := validateHTTPMethod(opts.method); err != nil {
		errorAndExit("%v\n", err)
	}

	parsed, errs := parseURLs(opts.urls)
	if len(errs) > 0 {
		for _, err := range errs {
			printError("%s\n", err)
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

	// First run before the ticker
	for _, url := range opts.urls {
		go opts.checkURL(client, url, results)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, url := range parsed {
				go opts.checkURL(client, url.String(), results)
			}
		case res := <-results:
			if err := res.log(opts.output); err != nil {
				errorAndExit("%v\n", err)
			}
		case <-quit:
			fmt.Fprintln(os.Stderr, "upmon: shutting down...")
			return
		}
	}
}

func (r result) log(format string) error {
	var err error
	switch format {
	case "json", "json-pretty":
		err = r.logJSON(format == "json-pretty")
	case "text":
		r.logText()
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

func (r result) logText() {
	fmt.Printf("time=%s url=%s status=%d latency=%s error=%q\n",
		r.Time,
		r.URL,
		r.Status,
		r.Latency,
		r.Error,
	)
}

type result struct {
	Time    string `json:"time"`
	URL     string `json:"url"`
	Status  int    `json:"status"`
	Latency string `json:"latency"`
	Error   string `json:"error"`
}

// checkURL performs an HTTP request to the given URL and sends the
// result to the provided channel.
func (o options) checkURL(client *http.Client, url string, ch chan<- result) {
	req, err := http.NewRequest(o.method, url, nil)
	if err != nil {
		ch <- result{Time: timestamp(), URL: url, Error: err.Error()}
		return
	}
	req.Header.Set("User-Agent", "upmon/"+version)

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		ch <- result{Time: timestamp(), URL: url, Error: err.Error()}
		return
	}
	defer resp.Body.Close()

	if !okResponse(resp.StatusCode) {
		ch <- result{
			Time:    timestamp(),
			URL:     url,
			Status:  resp.StatusCode,
			Latency: getLatency(start),
			Error:   http.StatusText(resp.StatusCode),
		}
		return
	}

	if o.quiet {
		// Suppress non-error messages in quiet mode
		return
	}

	ch <- result{
		Time:    timestamp(),
		URL:     url,
		Status:  resp.StatusCode,
		Latency: getLatency(start),
		Error:   "",
	}
}

func getLatency(start time.Time) string {
	return time.Since(start).Round(time.Millisecond).String()
}

func okResponse(status int) bool {
	return status >= 200 && status < 300
}

func validateInterval(interval time.Duration) error {
	if interval <= 0 {
		return errors.New("interval must be greater than zero")
	}
	return nil
}

func validateHTTPMethod(method string) error {
	switch method {
	case http.MethodGet, http.MethodHead:
		return nil
	default:
		return fmt.Errorf("invalid HTTP method: %s", method)
	}
}

func validateOutputFormat(format string) error {
	switch format {
	case "text", "json", "json-pretty":
		return nil
	default:
		return fmt.Errorf("unknown output format: %q", format)
	}
}

func parseURLs(urls []string) ([]*url.URL, []error) {
	var errs []error
	schemeRgx := regexp.MustCompile(urlSchemeRgx)

	parsed := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		url, err := url.ParseRequestURI(u)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		scheme := url.Scheme
		if !schemeRgx.MatchString(scheme) {
			errs = append(errs, fmt.Errorf("protocol %q not supported: %s", url.Scheme, url.String()))
			continue
		}
		parsed = append(parsed, url)
	}
	return parsed, errs
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

func usageAndExit(format string, a ...any) {
	printError(format+"\n", a...)
	flag.Usage()
	os.Exit(1)
}

func fatalError(format string, a ...any) {
	printError("error: "+format, a...)
	os.Exit(1)
}
