# upmon

upmon is a small CLI program that monitors the response status of websites at regular intervals.

## Install

```bash
git clone https://github.com/dusanbrankov/upmon.git
cd upmon
go install .
```

## Usage

```
Usage: upmon [option]... <url>...
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
  -q    Suppress non-error messages
  -h    Show this help message
```

## TODO

- [ ] Allow passing URL list and config as (JSON) file
- [ ] Let user control which fields to include in logs
