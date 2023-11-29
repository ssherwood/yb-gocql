module yb-gocql

go 1.20

require (
	github.com/gorilla/mux v1.8.0
	github.com/yugabyte/gocql v0.0.0-20230831121436-1e2272bb6bb6
	github.com/yugabyte/pgx/v4 v4.14.8
)

require (
	github.com/gocql/gocql v1.0.0 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.11.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.10.0 // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

//// testing tedyu's branch
//replace (
//	github.com/yugabyte/gocql => /home/lurtz/Repos/go/gocql
//)
