package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/yugabyte/gocql"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Println("Starting...")

	session := getYCQLSession()
	defer session.Close()

	router := mux.NewRouter()
	router.HandleFunc("/init", func(w http.ResponseWriter, r *http.Request) { InitData(session, w, r) }).Methods("GET")
	router.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { Search(session, w, r) }).Methods("GET")
	router.HandleFunc("/find/{id}", func(w http.ResponseWriter, r *http.Request) { FindById(session, w, r) }).Methods("GET")

	// serve the app
	fmt.Println("Server at 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func getYCQLSession() *gocql.Session {
	cluster := gocql.NewCluster("127.0.0.1") //, "127.0.0.2", "127.0.0.3")
	cluster.Port = 9042
	cluster.ProtoVersion = 4
	//cluster.DisableSkipMetadata = false // TODO why did we change the defaults?
	// TODO is this CQLVersion correct?
	cluster.CQLVersion = "3.4.2"               // CQL version (default: 3.0.0)
	cluster.ConnectTimeout = 12 * time.Second  // initial connection timeout, used during initial dial to server (default: 600ms)
	cluster.SocketKeepalive = 10 * time.Second // The keepalive period to use, enabled if > 0 (default: 0)
	cluster.Timeout = 12 * time.Second         // connection timeout (default: 600ms)
	cluster.Consistency = gocql.Quorum         // default consistency level (default: Quorum); "One" enables follow reads
	//cluster.Keyspace = "default" <- causes a segv on tserver

	// compression algorithm (default: nil)
	//cluster.Compressor = gocql.SnappyCompressor{}
	// ^ causes errors in the log, does YB support compression?

	// This uses the partition key "awareness" to route requests to the correct tserver host
	// It falls back to a datacenter aware round-robin approach...
	// check pool->hostConnPools
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("cloud1.datacenter1.rack1"))
	//cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("fooo")

	// Retry policy: https://pkg.go.dev/github.com/gocql/gocql#hdr-Retries_and_speculative_execution
	// BE CAREFUL WITH THIS...
	// Retry on reads w/ an aggressive cluster.Timeout can lead to a request storm during leader elections...
	//cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
	//	NumRetries: 5,
	//	Min:        50 * time.Millisecond, // not sure how this works, it's not really documented?
	//	Max:        2 * time.Second,
	//}

	// Default reconnection policy to use for reconnecting before trying to mark host as down (default: see below)
	//cluster.ReconnectionPolicy

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Connection error: ", err)
	}

	// TODO, what is this supposed to show?
	session.SetTrace(gocql.NewTraceWriter(session, os.Stdout))
	return session
}

func InitData(session *gocql.Session, _response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	queryParams := request.URL.Query()

	log.Println("Called InitData")
	log.Println("Vars:", vars)
	log.Println("Params:", queryParams)

	dataRows := 10_000 // default to 10,000 rows
	if v, exists := queryParams["rows"]; exists {
		if count, err := strconv.Atoi(v[0]); err == nil {
			dataRows = count
		}
	}

	// set up the demo keyspace
	log.Println("Initializing 'demo' KEYSPACE...")
	if err := session.Query("CREATE KEYSPACE IF NOT EXISTS demo").Exec(); err != nil {
		log.Fatal(err)
	}

	// set up the demo table
	log.Println("Initializing 'demo' TABLE...")
	if err := session.Query(`CREATE TABLE
                                 IF NOT EXISTS demo.demo(
									primaryId text,
									secondaryId text,
									clusterCol1 text,
									clusterCol2 text,
									dataCol1 int,
									dataCol2 boolean,
									dataCol3 timestamp,
									PRIMARY KEY((primaryId, secondaryId), clusterCol1, clusterCol2)
                                 )`).Exec(); err != nil {
		log.Fatal("create table if not exists:", err)
	}

	// load initial data
	log.Println("Initializing dataset with", dataRows, "(x2) rows...")

	for i := 0; i < dataRows; i++ {
		for j := 0; j < 2; j++ {
			if err := session.Query(`INSERT INTO demo.demo(
										primaryId, secondaryId,
										clusterCol1, clusterCol2,
										dataCol1, dataCol2,	dataCol3)
									       VALUES (?, ?, ?, ?, ?, ?, ?)`,
				"P1", fmt.Sprintf("%013d", i), fmt.Sprintf("FOO%d", j), "BAR", rand.Intn(100), rand.Intn(2) == 1, time.Now().UTC()).Exec(); err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Println("Initialization done!")
}

func Search(session *gocql.Session, response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	queryParams := request.URL.Query()

	log.Println("Called Search")
	log.Println("Vars:", vars)
	log.Println("Params:", queryParams)

	query := strings.Replace(
		`SELECT secondaryId, clusterCol1, clusterCol2, dataCol1, dataCol2, dataCol3
                FROM demo.demo
               WHERE primaryId = ? AND secondaryId IN(:ids)`,
		":ids", strings.Repeat("?,", 9)+"?", -1)

	params := []interface{}{"P1"}
	// TODO make this dynamic on queryParams
	for i := 0; i < 10; i++ {
		params = append(params, fmt.Sprintf("%013d", rand.Intn(1000)))
	}

	iter := session.Query(query, params...).Iter()

	var resultData []ResultData
	var secondaryId string
	var clusterCol1 string
	var clusterCol2 string
	var dataCol1 int
	var dataCol2 bool
	var dataCol3 time.Time

	for iter.Scan(&secondaryId, &clusterCol1, &clusterCol2, &dataCol1, &dataCol2, &dataCol3) {
		resultData = append(resultData, ResultData{
			SecondaryId: secondaryId,
			ClusterCol1: clusterCol1,
			ClusterCol2: clusterCol2,
			DataCol1:    dataCol1,
		})
	}

	var jsonResponse JsonResponse
	if err := iter.Close(); err != nil {
		log.Println(err)
		jsonResponse = JsonResponse{Type: "error", Message: err.Error()}
	} else {
		jsonResponse = JsonResponse{Type: "success", Data: resultData}
	}

	_ = json.NewEncoder(response).Encode(jsonResponse)
}

// FindById will return results that match the given PK
func FindById(session *gocql.Session, response http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	queryParams := request.URL.Query()

	log.Println("Called FindById")
	log.Println("Vars:", vars)
	log.Println("Params:", queryParams)

	primaryKeyLookup := "P1"
	secondaryKeyLookup := "0000000000000"

	if v, exists := vars["id"]; exists {
		if s, err := strconv.Atoi(v); err == nil {
			secondaryKeyLookup = fmt.Sprintf("%013d", s)
		}
	}

	query := `SELECT secondaryId, clusterCol1, clusterCol2, dataCol1, dataCol2, dataCol3
                FROM demo.demo
               WHERE primaryId = ? AND secondaryId = ?`

	iter := session.Query(query, primaryKeyLookup, secondaryKeyLookup).Iter()

	var resultData []ResultData
	var secondaryId string
	var clusterCol1 string
	var clusterCol2 string
	var dataCol1 int
	var dataCol2 bool
	var dataCol3 time.Time

	for iter.Scan(&secondaryId, &clusterCol1, &clusterCol2, &dataCol1, &dataCol2, &dataCol3) {
		resultData = append(resultData, ResultData{
			SecondaryId: secondaryId,
			ClusterCol1: clusterCol1,
			ClusterCol2: clusterCol2,
			DataCol1:    dataCol1,
			DataCol2:    dataCol2,
			DataCol3:    dataCol3,
		})
	}

	var jsonResponse JsonResponse
	if err := iter.Close(); err != nil {
		log.Println(err)
		jsonResponse = JsonResponse{Type: "error", Message: err.Error()}
	} else {
		jsonResponse = JsonResponse{Type: "success", Data: resultData}
	}

	_ = json.NewEncoder(response).Encode(jsonResponse)
}

type JsonResponse struct {
	Type    string       `json:"type"`
	Data    []ResultData `json:"data"`
	Message string       `json:"message"`
}

type ResultData struct {
	PrimaryId   string    `json:"primary_id"`
	SecondaryId string    `json:"secondary_id"`
	ClusterCol1 string    `json:"cluster_col_1"`
	ClusterCol2 string    `json:"cluster_col_2"`
	DataCol1    int       `json:"data_col_1"`
	DataCol2    bool      `json:"data_col_2"`
	DataCol3    time.Time `json:"data_col_3"`
}
