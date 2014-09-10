package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/codegangsta/negroni"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime"
	"strings"
	"time"
)

var buildVersion string

type (
	ProxyHandler struct {
		serveMux *http.ServeMux
	}

	RateLimiter struct {
		Requests chan *Request
	}

	DB struct {
		*sql.DB
	}

	Tx struct {
		*sql.Tx
	}

	Record struct {
		ID    int
		Email string
		IP    string
		Count int
		Date  int64
	}

	Request struct {
		ResponseWriter http.ResponseWriter
		HTTPRequest    *http.Request
		Result         chan bool
	}
)

func NewProxyHandler(upstream *url.URL) *ProxyHandler {
	serveMux := http.NewServeMux()
	serveMux.Handle("/", httputil.NewSingleHostReverseProxy(upstream))
	return &ProxyHandler{serveMux}
}

func (h *ProxyHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	h.serveMux.ServeHTTP(rw, req)
}

func NewDBConn(uri string) (*DB, error) {
	db, err := sql.Open("sqlite3", uri)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) CreateLayout() error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS records (
id INTEGER PRIMARY KEY AUTOINCREMENT,
email VARCHAR,
ip VARCHAR,
count INTEGER DEFAULT '0',
date INTEGER
)`)
	if err != nil{
		return err
	}
	return nil
}

func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (tx *Tx) GetRecord(email, ip string, ts int64) (*Record, error) {
	stmt, err := tx.Prepare(`SELECT * FROM records
WHERE records.email=?
AND records.ip=?
AND DATE(?, 'unixepoch')=DATE('now')
LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	record := &Record{}
	if err := stmt.QueryRow(email, ip, ts).Scan(&record.ID, &record.Email, &record.IP, &record.Count, &record.Date); err != nil {
		return nil, err
	}
	// please check for sql.ErrNoRows
	return record, nil
}

func (tx *Tx) NewRecord(email, ip string, ts int64) error {
	stmt, err := tx.Prepare(`INSERT INTO records (email, ip, date)
VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(email, ip, ts); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) IncRecord(email, ip string, ts int64) error {
	stmt, err := tx.Prepare(`UPDATE records SET count=count+1
WHERE records.email=?
AND records.ip=?
AND DATE(?, 'unixepoch')=DATE('now')`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(email, ip, ts); err != nil {
		return err
	}
	return nil
}

func NewRateLimitMiddleWare() *RateLimiter {
	requests := RateLimitWorkerPool()
	return &RateLimiter{requests}
}

func (m *RateLimiter) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
	request := &Request{
		ResponseWriter: rw,
		HTTPRequest:    req,
		Result:         make(chan bool),
	}
	m.Requests <- request
	ok := <-request.Result
	if ok {
	}

	next(rw, req)
}

func RateLimitWorkerPool() chan *Request {
	requests := make(chan *Request)
	cpuCount := runtime.NumCPU()
	for i := 0; i < cpuCount; i++ {
		db, err := NewDBConn("record.db")
		if err != nil {
			log.Fatal(err.Error())
		}

		if err := db.Ping(); err != nil {
			log.Fatal(err.Error())
		}

		if err := db.CreateLayout(); err != nil {
			log.Fatal(err.Error())
		}

		go RateLimitWorker(requests, db)
	}
	return requests
}

func RateLimitWorker(requests chan *Request, db *DB) {
	for request := range requests {
		ip := request.HTTPRequest.RemoteAddr
		ip = strings.Split(ip, ":")[0]
		email := request.HTTPRequest.Header.Get("X-Forwarded-Email")
		ts := time.Now().Unix()

		tx, err := db.Begin()
		if err != nil {
			log.Print(err.Error())
		}

		var count int
		record, err := tx.GetRecord(email, ip, ts)
		if err != nil {
			if err == sql.ErrNoRows {
				if err := tx.NewRecord(email, ip, ts); err != nil {
					log.Print(err.Error())
					// Pass through if error
					tx.Rollback()
					request.Result <- true
					continue
				}
				count = 0
			} else {
				log.Print(err.Error())
				// Pass through if error
				tx.Rollback()
				request.Result <- true
				continue
			}
		} else {
			count = record.Count
		}

		log.Printf("ip: %s, email: %s, count: %d", ip, email, count)

		if err := tx.IncRecord(email, ip, ts); err != nil {
			log.Print(err.Error())
			tx.Rollback()
			request.Result <- true
			continue
		}

		if err := tx.Commit(); err != nil {
			tx.Rollback()
			log.Print(err.Error())
		}

		// TODO return false if limit exceeded
		request.Result <- true
	}
}

func main() {
	fmt.Printf("hyperion%s\n", buildVersion)

	httpAddr := flag.String("http-address", "127.0.0.1:4765", "<addr>:<port> to listen on")
	upstream := flag.String("upstream", "", "http url for the upstream endpoint")
	flag.Parse()

	if *upstream == "" {
		log.Fatal("--upstream not found")
		flag.Usage()
	}

	upstreamURL, err := url.Parse(*upstream)
	if err != nil {
		log.Fatalf("invalid --upstream (%s) %s", upstream, err.Error())
		flag.Usage()
	}

	recovery := negroni.NewRecovery()
	logger := negroni.NewLogger()
	proxyHandler := NewProxyHandler(upstreamURL)
	rateLimiter := NewRateLimitMiddleWare()

	n := negroni.New(recovery, logger)
	n.Use(rateLimiter)
	n.UseHandler(proxyHandler)
	n.Run(*httpAddr)
}
