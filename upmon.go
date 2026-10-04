package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	userAgent = "upmon/0.0.1"
)

var ErrUnknownFormat = errors.New("unknown output format")

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

type options struct {
	urls     []string
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
		usageAndExit("at least one URL must be provided")
	}

	if err := validateInterval(opts.interval); err != nil {
		errorAndExit(err)
	}

	if err := validateOutputFormat(opts.output); err != nil {
		errorAndExit(err)
	}

	opts.method = strings.ToUpper(opts.method)

	if err := validateHTTPMethod(opts.method); err != nil {
		errorAndExit(err)
	}

	parsed, errs := parseURLs(opts.urls)
	if len(errs) > 0 {
		for _, err := range errs {
			printError("%s\n", err)
		}
		os.Exit(1)
	}

	client := newHTTPClient()

	results := make(chan result, len(opts.urls))

	checkURLs := func() {
		for _, url := range parsed {
			go opts.checkURL(client, url.String(), results)
		}
	}

	// First run before the ticker
	checkURLs()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			checkURLs()
		case res := <-results:
			if err := res.log(os.Stdout, opts.output); err != nil {
				errorAndExit(err)
			}
		case <-quit:
			fmt.Fprintln(os.Stderr, "upmon: shutting down...")
			return
		}
	}
}

func (r result) log(w io.Writer, format string) error {
	switch format {
	case "json", "json-pretty":
		return r.logJSON(w, format == "json-pretty")
	case "text":
		return r.logText(w)
	default:
		return ErrUnknownFormat
	}
}

func (r result) logJSON(w io.Writer, pretty bool) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(r)
}

func (r result) logText(w io.Writer) error {
	_, err := fmt.Fprintf(w, "time=%s url=%s status=%d latency=%s error=%q\n",
		r.Time,
		r.URL,
		r.Status,
		r.Latency,
		r.Error,
	)
	return err
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
	req.Header.Set("User-Agent", userAgent)

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		ch <- result{
			Time:    timestamp(),
			URL:     url,
			Latency: getLatency(start),
			Error:   err.Error(),
		}
		return
	}
	resp.Body.Close()

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

	parsed := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		parsedURL, err := url.ParseRequestURI(u)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		switch parsedURL.Scheme {
		case "http", "https":
			// valid schemes
		default:
			errs = append(errs, fmt.Errorf("unsupported URL scheme: %s", parsedURL.Scheme))
			continue
		}
		if parsedURL.Hostname() == "" {
			errs = append(errs, fmt.Errorf("URL has no host: %s", u))
			continue
		}
		parsed = append(parsed, parsedURL)
	}
	return parsed, errs
}

func newHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()

	t.DisableCompression = true
	t.MaxConnsPerHost = 2
	t.ResponseHeaderTimeout = 5 * time.Second
	t.TLSHandshakeTimeout = 5 * time.Second

	return &http.Client{
		Transport: t,
		Timeout:   10 * time.Second,
	}
}

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func printError(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "upmon: "+format, a...)
}

func errorAndExit(err error) {
	printError("%v\n", err)
	os.Exit(1)
}

func usageAndExit(format string, a ...any) {
	if format != "" {
		printError(format+"\n\n", a...)
	}
	flag.Usage()
	os.Exit(1)
}
