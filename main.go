package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/codegangsta/negroni"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
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
		Date  string
	}

	Request struct {
		ResponseWriter http.ResponseWriter
		HTTPRequest    *http.Request
		// Result         chan bool
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

func NewDBConn(dsn string) (*DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func LayoutDB(dsn string) error {
	db, err := NewDBConn(dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := tx.CreateLayout(); err != nil {
		log.Print("double")
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Print("triple")
		tx.Rollback()
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

func (tx *Tx) CreateLayout() error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS records (
id SERIAL,
email VARCHAR(100),
ip VARCHAR(45),
count INTEGER UNSIGNED DEFAULT 0,
date DATE
) ENGINE = InnoDB`)
	if err != nil {
		return err
	}

	return nil
}

func (tx *Tx) GetRecord(email, ip, date string) (*Record, error) {
	stmt, err := tx.Prepare(`SELECT * FROM records
WHERE records.email=?
AND records.ip=?
AND records.date=?
LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	record := &Record{}
	if err := stmt.QueryRow(email, ip, date).Scan(&record.ID, &record.Email, &record.IP, &record.Count, &record.Date); err != nil {
		return nil, err
	}
	// please check for sql.ErrNoRows
	return record, nil
}

func (tx *Tx) NewRecord(email, ip, date string) error {
	stmt, err := tx.Prepare(`INSERT INTO records (email, ip, date)
VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(email, ip, date); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) IncRecord(email, ip, date string) error {
	stmt, err := tx.Prepare(`UPDATE records SET count=count+1
WHERE records.email=?
AND records.ip=?
AND records.date=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.Exec(email, ip, date); err != nil {
		return err
	}
	return nil
}

func NewRateLimitMiddleWare(dsn string) *RateLimiter {
	requests := RateLimitWorkerPool(dsn)
	return &RateLimiter{requests}
}

func (m *RateLimiter) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
	request := &Request{
		ResponseWriter: rw,
		HTTPRequest:    req,
		// Result:         make(chan bool),
	}
	m.Requests <- request
	// TODO Later when block is needed
	// ok := <-request.Result
	// if ok {
	// }

	next(rw, req)
}

func RateLimitWorkerPool(dsn string) chan *Request {
	requests := make(chan *Request)

	db, err := NewDBConn(dsn)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err.Error())
	}

	go RateLimitWorker(requests, db)

	return requests
}

func RateLimitWorker(requests chan *Request, db *DB) {
	for request := range requests {
		ip := request.HTTPRequest.Header.Get("X-Forwarded-For")
		email := request.HTTPRequest.Header.Get("X-Forwarded-Email")
		date := strings.Split(time.Now().String(), " ")[0]

		tx, err := db.Begin()
		if err != nil {
			log.Print(err.Error())
		}

		// TODO Implement when blocking is needed
		// var count int
		// record, err := tx.GetRecord(email, ip, date)
		_, err = tx.GetRecord(email, ip, date)
		if err != nil {
			if err == sql.ErrNoRows {
				if err := tx.NewRecord(email, ip, date); err != nil {
					log.Print(err.Error())
					// Pass through if error
					tx.Rollback()
					// request.Result <- true
					continue
				}
				// count = 0
			} else {
				log.Print(err.Error())
				// Pass through if error
				tx.Rollback()
				// request.Result <- true
				continue
			}
		} else {
			// count = record.Count
		}

		// Debugging
		// log.Printf("ip: %s, email: %s, count: %d", ip, email, count)

		if err := tx.IncRecord(email, ip, date); err != nil {
			log.Print(err.Error())
			tx.Rollback()
			// request.Result <- true
			continue
		}

		if err := tx.Commit(); err != nil {
			tx.Rollback()
			log.Print(err.Error())
		}

		// TODO return false if limit exceeded
		// request.Result <- true
	}
}

func main() {
	fmt.Printf("hyperion%s\n", buildVersion)

	httpAddr := flag.String("http-address", "127.0.0.1:4765", "<addr>:<port> to listen on")
	upstream := flag.String("upstream", "", "http url for the upstream endpoint")
	dsn := flag.String("db", "", "Database source name")
	flag.Parse()

	if *upstream == "" {
		flag.Usage()
		log.Fatal("--upstream not found")
	}

	dataSource := *dsn
	if dataSource == "" {
		if os.Getenv("HYPERION_DB") != "" {
			dataSource = os.Getenv("HYPERION_DB")
		}
	}

	if dataSource == "" {
		flag.Usage()
		log.Fatal("--db or HYPERION_DB not found")
	}

	upstreamURL, err := url.Parse(*upstream)
	if err != nil {
		flag.Usage()
		log.Fatalf("invalid --upstream (%s) %s", upstream, err.Error())
	}

	if err := LayoutDB(dataSource); err != nil {
		log.Fatal(err)
	}

	recovery := negroni.NewRecovery()
	logger := negroni.NewLogger()
	proxyHandler := NewProxyHandler(upstreamURL)
	rateLimiter := NewRateLimitMiddleWare(dataSource)

	n := negroni.New(recovery, logger)
	n.Use(rateLimiter)
	n.UseHandler(proxyHandler)
	n.Run(*httpAddr)
}
