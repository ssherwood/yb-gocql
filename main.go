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

func main() {
	log.Println("Starting...")
	//pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))

	defaultLogger := log.Default()
	session := getYCQLSession(defaultLogger)
	defer session.Close()

	router := mux.NewRouter()
	router.HandleFunc("/init", func(w http.ResponseWriter, r *http.Request) { InitData(session, w, r) }).Methods("POST")
	router.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) { Search(session, w, r) }).Methods("GET")
	router.HandleFunc("/find/{id}", func(w http.ResponseWriter, r *http.Request) { FindById(session, w, r) }).Methods("GET")

	// serve the app
	log.Println("Server at 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func getYCQLSession(log *log.Logger) *gocql.Session {
	contactPoints := strings.Split(strings.ReplaceAll(GetEnv("YCQL_CONTACT_POINTS", "127.0.0.1,127.0.0.2,127.0.0.3"), " ", ""), ",")
	cluster := gocql.NewCluster(contactPoints...)

	cluster.Logger = log // TODO how to do better logging?
	cluster.Port = GetEnv("YCQL_PORT", 9042)
	cluster.CQLVersion = GetEnv("YCQL_VERSION", "3.4.2")   // CQL version (default: 3.0.0)
	cluster.ProtoVersion = GetEnv("YCQL_PROTO_VERSION", 4) // Native protocol version

	// initial connection timeout, used during initial dial to server (default: 600ms)
	cluster.ConnectTimeout = GetEnv[time.Duration]("YCQL_CONNECT_TIMEOUT", 3) * time.Second
	// query timeout (default: 600ms)
	cluster.Timeout = GetEnv[time.Duration]("YCQL_QUERY_TIMEOUT", 1500) * time.Millisecond
	// The keepalive period to use, enabled if > 0 (default: 0) [default Dial default is 15s?]
	cluster.SocketKeepalive = GetEnv[time.Duration]("YCQL_SOCKET_KEEPALIVE", 15) * time.Second
	// default consistency level (default: Quorum); "One" enables follow reads
	cluster.Consistency = gocql.ParseConsistency(GetEnv[string]("YCQL_CONSISTENCY", "LOCAL_ONE"))
	// really no parse for SerialConsistency?
	cluster.SerialConsistency = gocql.Serial

	//cluster.Keyspace = "demo"
	//cluster.DisableSkipMetadata = true 	   // TODO why did we change the defaults? https://github.com/yugabyte/yugabyte-db/issues/1312

	// compression algorithm (default: nil)
	//cluster.Compressor = gocql.SnappyCompressor{}

	// This uses the partition awareness to route requests to the tablet leaders tserver
	// It falls back to a datacenter aware round-robin approach...
	cluster.PoolConfig.HostSelectionPolicy = gocql.YBPartitionAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.NumConns = 5 // number of connections per host (default 2)
	// use a filter if you want to restrict connections to local DC nodes
	//cluster.HostFilter = gocql.DataCentreHostFilter("us-east2")

	// Retry policy: https://pkg.go.dev/github.com/gocql/gocql#hdr-Retries_and_speculative_execution
	// BE CAREFUL WITH THIS...
	// Retry on reads w/ an aggressive cluster.Timeout can lead to a request storm during leader elections...
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 3,
		Min:        50 * time.Millisecond, // not sure how this works, it's not really documented?
		Max:        2 * time.Second,
	}

	// Default reconnection policy to use for reconnecting before trying to mark host as down (default: see below)
	//cluster.ReconnectionPolicy

	// TODO
	//cluster.SslOpts = &gocql.SslOptions{
	//	EnableHostVerification: true,
	//}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Connection error: ", err)
	}

	// TODO, what is this supposed to show?
	session.SetTrace(gocql.NewTraceWriter(session, log.Writer()))

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

	//primaryKeysId, exists := queryParams["pk1"]
	//if !exists {
	//	// todo error
	//}

	//secondaryKeysId, exists := queryParams["pk2"]
	//if !exists {
	//	// todo error
	//}

	const query = //strings.Replace(
	`SELECT secondaryId, clusterCol1, clusterCol2, dataCol1, dataCol2, dataCol3
                FROM demo.demo
               WHERE primaryId = :primaryKeyId AND secondaryId IN :ids`
	//":ids", strings.Repeat("?,", 9)+"?", -1)

	// TODO not yet handling multiple PKs
	ids := make([]string, 10)

	// TODO make this dynamic on queryParams
	for i := 0; i < 10; i++ {
		//ids = append(ids, fmt.Sprintf("%013d", rand.Intn(10_000)))
		ids[i] = fmt.Sprintf("%013d", rand.Intn(10_000))
	}

	primaryKey := "P1"
	iter := session.Query(query, primaryKey, ids).Iter()

	var (
		resultData  []ResultData
		secondaryId string
		clusterCol1 string
		clusterCol2 string
		dataCol1    int
		dataCol2    bool
		dataCol3    time.Time
	)

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

	// this is still not optimal:
	// Range Scan on demo.demo
	//   Key Conditions: (primaryid = 'P1') AND (secondaryid = '0000000000000')
	query := `SELECT secondaryId, clusterCol1, clusterCol2, dataCol1, dataCol2, dataCol3
                FROM demo.demo
               WHERE primaryId = :primaryKeyLookup AND secondaryId = :secondaryKeyLookup`

	iter := session.Query(query, primaryKeyLookup, secondaryKeyLookup).Iter()

	var (
		resultData  []ResultData
		secondaryId string
		clusterCol1 string
		clusterCol2 string
		dataCol1    int
		dataCol2    bool
		dataCol3    time.Time
	)

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

// GetEnv
// TODO move to a utility package
func GetEnv[T any](env string, defaultValue T) T {
	envValue, ok := os.LookupEnv(env)
	if !ok {
		return defaultValue
	}

	returnValue := defaultValue
	switch p := any(&returnValue).(type) {
	case *string:
		*p = envValue
	case *int:
		*p, _ = strconv.Atoi(envValue)
	case *time.Duration:
		val, _ := strconv.Atoi(envValue)
		*p = time.Duration(val)
	}

	return returnValue
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
