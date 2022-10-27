package main

import (
	"log"
	"fmt"
	"math"
	"time"
	"context"
	"net/http"
	"encoding/json"
	
	"database/sql"
	_ "github.com/lib/pq"

	"glassnode/lib/params"
	"glassnode/lib/common"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	
	"github.com/fasthttp/websocket"

	"github.com/davecgh/go-spew/spew"
)

/* Puller class */
type Puller struct {
	// Contracts
	Contracts map[string]int64
	ContractMaxTime int64

	// Main methods
	CacheContracts func (puid string, to int64) bool
	CachingContracts chan bool

	Pull func (puid string, from, to int64) (*map[int64]float64, bool)
}

func main () {

	/* Params (env) */
	_params := params.Get ()

	/*
		The problem: 
			1) I've got an example database. I mean this app to work on the full-scale database
			2) With more than 10^9 txs from the beginning, MAYBE this is not a good idea to use Postgres as a intermediary at all.
			   That's why I've created a distinct pulling microservice (it may work with direct access to eth full node on some internal server, for example, or some self-made binary cache)
			3) This (distinct) microservice is currently represented in Puller class
			   So if we finally won't use distinct puller microservice, then it's better to use a class in case of gathering all of the code in one source file
			4) Obviously this solution is useless without 'from' and 'to' HTTP parameters in query (meaning full-scale DB) and some time range limitation as well
			5) I'm using fixed data resolution so it allows me to dynamically precache everything I need on demand, based on users' queries
			6) I need to pipeline those queries in order of avoiding time range overlaps during dynamic cache building process (and to control DB/microservice loading to)
			7) I mean this solution for really hard parallel querying, nevertheless, there're always many ways to optimize it further

		TODO:
		    - Specific error codes/texts
		    - Flag package instead of constants (command line arguments)
		    - Peak loading test on full-scale data is required
		    - Comprehensive logging and status system are required
		      - Bools or errors? (negotiable; I normally use bools & logs)
    	    - Other @TODO's, @SUGGESTION's below
		
		Additional suggestions:
		    - Use higher precision timestamps (milliseconds at least)
		    - Optimize DB (indexes)
	*/

	/* Puller */
	// Initialization
	puller := Puller {}
	puller.Contracts = make (map[string]int64)

	/* DB */
	// So it's required to parallelize DB connections for highload usage
	connStr := fmt.Sprintf ("host=%s port=%d user=%s "+
        "password=%s dbname=%s sslmode=disable",
        	_params.Host, _params.Port, _params.User, _params.Password, _params.DBName)

    // Making the distinct DB connection for CacheContracts method, because it is single-threaded and called all the time
    var pdb *sql.DB
	for {
		pdb, _ = sql.Open ("postgres", connStr)
		if pdb.Ping () != nil {
	        log.Println ("[puller] Connecting to DB...")
	        time.Sleep (time.Second)
	        continue
	    }
	    break
	}

	defer pdb.Close ()

	// Connections pool for the main method
	config, err := pgxpool.ParseConfig (connStr)
	if err != nil {
		panic (err)
	}

	// Additional configuration
	config.MaxConnIdleTime = time.Minute * time.Duration (_params.PoolTimeoutM)
	config.MaxConns = int32 (_params.PoolParallel)
	config.MinConns = 1
	config.HealthCheckPeriod = time.Minute
	
	// Starting the pool
	pool, err := pgxpool.ConnectConfig (context.Background (), config)
	if err != nil {
		panic (err)
	}

	defer pool.Close ()
	
	/*
		Precache method for contract addresses
		IMPORTANT! This method can NOT be called concurrently, one thread only!
		@TODO it's obviously a bottleneck for highload usage (and a time overlap can not be avoided at the moment)
		@SUGGESTION it'd be great to rebuild the original Docker image in order of adding an incremental 'uid' field into the 'contracts' table
	 			    in this case I can avoid duplicate queries to this table, overlapped by timestamp
	 			    of course I can add a hotfix and patch this table from here
	 			    but it will still be required to use this field in the app that's adding new rows to this table
	 			    because of that, I consider such a hotfix as an inappropriate solution
	*/		
	
	// Mutex init
	puller.CachingContracts = make (chan bool, 1)

	// The method
	puller.CacheContracts = func (puid string, to int64) bool {

		// One-thread mutex
		puller.CachingContracts <- true

		/*
			Pulling contract addresses (up to the time)
			It's okay to cache them all in mem, we can use millions of them if required
			Maps in go 10x slower than lists, but at the moment I will not experiment with custom binary search and stuff
			Of course, we have more than 18k contracts on Eth
			I've got the problem's point as "we need it all instead of contract creation txs and txs with some specific type of contracts you don't want to see"
			So this is the only way possible to understand it because otherwise I have to parse ALL contract addresses from infura (for example) at least until 2020-09-07
			I'll do it easily and with my pleasure but it will take several days more (with peak loading tests as well)
		*/

		// Query
		_cf := common.TimeToStr (puller.ContractMaxTime)
		_to := common.TimeToStr (to)

		if _params.Verbose {
			log.Printf ("[%s] Pulling all contracts from '%s' to '%s'", puid, _cf, _to)
		}

		rows, err := pdb.Query (fmt.Sprintf ("SELECT address,block_time FROM contracts WHERE block_time>='%s' AND block_time<='%s'", 
			_cf, _to))

		if err != nil {
			if _params.Verbose {
				log.Printf ("[%s] ERROR: query internal error", puid)
			}

			<- puller.CachingContracts
			return false
		}

		// Pulling
		var pulled, errors int = 0, 0

		for rows.Next () {
			var address, created string
			err := rows.Scan (&address, &created)

			if err != nil {
				errors ++
				continue
			}

			t, ok := common.StrToTime (created)
			
			if ok {
				puller.Contracts[address] = t
				pulled ++

				if t > puller.ContractMaxTime {
					puller.ContractMaxTime = t
				}
			} else {
				errors ++
			}
		}

		// Closing
		rows.Close ()
		
		if _params.Verbose {
			log.Printf ("[%s] - pulled %d contracts with %d errors, cached them; overall %d contracts in mem; max time is '%s'", 
				puid, pulled, errors, len (puller.Contracts), common.TimeToStr (puller.ContractMaxTime))
		}

		// Mutex off
		<- puller.CachingContracts

		return true
	}

	// So, precaching all contracts until now
	if !puller.CacheContracts ("init", time.Now ().Unix () +1) {
		log.Fatalln ("Can not precache contracts")
	}

	// Main puller method (CAN be called concurrently but without from-to overlapping, so that is checked on solution.go' level)
	// Firstly, out purprose is not to overload postgres so I'm creating a connection pool 

	// The method itself
	puller.Pull = func (puid string, from, to int64) (*map[int64]float64, bool) { 
		/* of course we can use 'func (puller *Puller) Pull (...) {...}' outside of main's body
		we'll need a simple constructor for 'puller.Contracts' in this case */
		
		_from := common.TimeToStr (from)
		_to := common.TimeToStr (to)

		if _params.Verbose {
			log.Printf ("[%s] Received a request from '%s' to '%s'", puid, _from, _to)
		}

		// Caching contracts (up to time specified)
		if !puller.CacheContracts (puid, to) {
			return nil, false
		}
		
		/* Pulling transactions in the time range specified */
		if _params.Verbose {
			log.Printf ("[%s] Pulling all transactions from '%s' to '%s'", puid, _from, _to)
		}

		// Query
		rows, err := pool.Query (context.TODO (), fmt.Sprintf ("SELECT block_time,\"from\",\"to\",gas_used,gas_price FROM %s WHERE block_time>='%s' AND block_time<='%s'", 
			_params.TableName, _from, _to))

		if err != nil {
			panic (err)
		}

		// Pulling
		var pulled, used, errors int = 0, 0, 0

		// ...to aggregated data
		aggr_eth := make (map[int64]float64)

		_OK := func (addr string, t int64) bool {
			if addr == "0x0000000000000000000000000000000000000000" {
				return false
			}

			if _, ex := puller.Contracts[addr]; ex {
				if puller.Contracts[addr] <= t {
					return false
				}
			}

			return true
		}

		_big := math.Pow (10, 18)

		for rows.Next () {
			// Reading
			values, err := rows.Values ()
		    if err != nil {
		    	errors ++
		    	continue
		    }

		    // Converting
		    _bt := 			values[0].(time.Time)
		    _tx_from :=	 	values[1].(string)
		    _tx_to :=	 	values[2].(string)
		    
		    var _gu, _gp uint64

		    tmp3 := values[3].(pgtype.Numeric)
		    tmp4 := values[4].(pgtype.Numeric)
		    
		    tmp3.AssignTo (&_gu)
		    tmp4.AssignTo (&_gp)
		    
		    _block_t := _bt.Unix ()
            _gas_gwei := _gu * _gp

            pulled ++
            
            // Filtering
            if _OK (_tx_from, _block_t) && _OK (_tx_to, _block_t) {
            	used ++
	           
	            /* Aggregating */
	            // To timestep
	            ts := _block_t
	            ts /= _params.Timestep
	            ts *= _params.Timestep 

	            // Mapping & summing
	            if _, ex := aggr_eth[ts]; !ex {
	            	aggr_eth[ts] = 0
	            }

	            aggr_eth[ts] += float64 (_gas_gwei) / _big
	        }
        }

        // Closing
        rows.Close ()

        if _params.Verbose {
			log.Printf ("[%s] - pulled %d transactions, %d used after filtration, %d errors", 
				puid, pulled, used, errors)
		}

		return &aggr_eth, true // so we're sending json.Marshal (aggr_eth) in case of microservice
	}

	/* Starting the WS server for internal puller microservice */
	var upgrader = websocket.Upgrader {}

	http.HandleFunc ("/", func (w http.ResponseWriter, req *http.Request) {
		
		// WS initialization
        c, err := upgrader.Upgrade (w, req, nil)
        if err != nil {
            return
        }
        defer c.Close ()

        // Serving
        for {

        	// Reading message
			_, message, err := c.ReadMessage ()

			if err != nil {
			    return // client've gone offline
			}

			// Unmarshalling
			ws_request := common.WSRequest {}
			err = json.Unmarshal (message, &ws_request)

			if err != nil {
				continue // wrong format
			}

			// Replying
			err = c.WriteMessage (1, common.QueryProcess (ws_request.From, ws_request.To, _params, func (from, to int64, response *common.RESTResponse) {

	        	// Pulling
	            aggr_eth, ok := puller.Pull (ws_request.UID, from, to)
	            
	            if !ok {
	                response.Error = "Internal pulling error"
	            } else {

	                // Success
	                response.Status = true
	                response.Data = common.InternalMessage {
	                	UID: ws_request.UID,
	                	Map: aggr_eth,
	                }
	            }
	        }))

	        if _params.Verbose {
				log.Printf ("[%s] Response sent", ws_request.UID)
			}

	        if err != nil {
			    return // client've gone offline
			}
        }
    })

    log.Printf ("Server started at '%s'", _params.PullerIPPort)

    go func () {
    	for {

    		log.Printf ("--- Pool status ---")
    		spew.Dump (pool.Stat ())
    		log.Printf ("-------------------")

    		time.Sleep (time.Minute)
    	}
    }()

    http.ListenAndServe (_params.PullerIPPort, nil)
}