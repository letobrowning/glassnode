package main

import (
	"log"
	"time"
	"sync"
	"sort"
	"strconv"
	"net/http"
	"encoding/json"

	"glassnode/lib/params"
	"glassnode/lib/common"

	"github.com/fasthttp/websocket"

	// "github.com/davecgh/go-spew/spew"
)

const BUF = 10000

type ResponseItem struct {
	TimeS int64 `json:"t"`
	Value float64 `json:"v"`
}

type Query struct {
	From int64
	To int64
	Sent int64
	Response chan *common.RESTResponse
}

func main () {

	/* Params (env) */
	_params := params.Get ()

	/* 
		Query queue
		So queries are parallel and contain 'from' and 'to' timestamps
		Parallel queries shall not overlap in their time range in order of avoiding duplicated DB load on puller.go
		Moreover, throttling is required too
	*/

	// WS puller microservice connection (stable, endless)
	in  := make (chan []byte, BUF)
	out := make (chan []byte, BUF)
	end := make (chan bool, 1)

	// Last disconnect time
	ld := int64 (0)
	ld_mutex := &sync.RWMutex {}

	// Method
	WS := func (href string, in * chan []byte, out * chan []byte, end * chan bool) {
	    
	    c, _, err := websocket.DefaultDialer.Dial (href, nil)
	    if err != nil {
	        log.Println ("[Websocket] [ERROR] WS dial:", err, "at", href)
	        time.Sleep (time.Second)
	        return
	    }
	    defer c.Close ()

	    go func () {

	    	for {
	    		if len (*in) > 0 {
	    			c.WriteMessage (1, <- *in)
	    		}

	    		if len (*end) > 0 {
	                return
	            }

	    		time.Sleep (time.Millisecond)
	    	}
	    }()

		log.Println ("[Websocket] Connected to", href)

	    for {
	        _, message, err := c.ReadMessage ()
	        if err != nil {
	            log.Println ("[Websocket] [ERROR] WS read:", err, "at", href)
	            
	            go func () {
	            	// Updating last WS disconnect time
	            	ld_mutex.Lock ()
	            	ld = time.Now ().Unix ()
	            	ld_mutex.Unlock ()

	            	// Stopping the routine above
	            	*end <- true
	            }()
	            
	            return
	        }

	        *out <- message

	        if len (*end) > 0 {
	            return
	        }
	    }
	}

	// Connecting
	go func () {
		for {
			WS ("ws://" + _params.PullerIPPort, &in, &out, &end)
		}
	}()

	// Queue
	queue := make (chan *Query, _params.SolutionParallel)

	// WS query pool
	pool := make (map[string]*Query)
	pool_mutex := &sync.RWMutex {}
	
	go func () {
		for {

			// Reading a query (splitted below)
			// One thread processing so there's no mutex
			if len (queue) > 0 { // @TODO switch = ?
				if len (pool) < _params.SolutionParallel {
					
					// Reading
					q := <- queue

					// Unique query ID
					var puid string

					for {
						puid = strconv.Itoa (int (time.Now ().UnixNano ()))
						if _, ex := pool[puid]; !ex {
							break
						}
						time.Sleep (time.Nanosecond)
					}
					
					// Adding query to the pool
					pool_mutex.Lock ()
					pool[puid] = q
					pool_mutex.Unlock ()
					
					// Making the query
					bin, _ := json.Marshal (&common.WSRequest {
						UID: puid,
						From: strconv.Itoa (int (q.From)),
						To: strconv.Itoa (int (q.To)),
					})

					if _params.Debug {
						log.Printf ("Request to puller: '%s'", string (bin))
					}

					in <- bin
				}			
			}

			// Processing responses
			if len (out) > 0 {
				response := common.RESTResponse {}
				
				b := <- out
				if _params.Debug {
					log.Printf ("Puller's response: '%s'", string (b))
				}

				json.Unmarshal (b, &response) // no err check, because UID'll be unknown in this case => RepeatTimeoutS

				// Converting data
				tmp, ok := response.Data.(map[string]interface {})

				if ok {
					_, ok1 := tmp["uid"]
					_, ok2 := tmp["map"]

					if ok1 && ok2 {

						// Parsing
						puid := tmp["uid"].(string)
						aggr_eth_i := tmp["map"].(map[string]interface {})

						// Building the final map
						aggr_eth := make (map[int64]float64)

						for _k, _v := range aggr_eth_i {
							k, e := strconv.Atoi (_k)
							
							if e == nil {
								aggr_eth[int64 (k)] = _v.(float64) // @TODO may panic

							} // @TODO rare error (Status=false?)
						}

						if _, ex := pool[puid]; ex {
							q := pool[puid]

							// Replying
							if len (q.Response) == 0 {
								q.Response <- &common.RESTResponse {
									Status: true,
									Data: &aggr_eth,
								}
							}

							// Deleting from the pool
							pool_mutex.Lock ()
							delete (pool, puid)
							pool_mutex.Unlock ()

						} // @TODO rare error
					} // else the same situation
				} // else = ?
			}
		}
	}()

	/*
		Local cache
		Obviosly we don't need to query again all aggregated data in the past
		Therefore, during dynamic cache filling all that required is fill the gaps in data, concurrently 
	*/

	cache := make (map[int64]float64)
	cache_mutex := &sync.RWMutex {}

	/*
	// @DEBUG
	// curl 'http://127.0.0.1:7001/?from=1599436800&to=1599523200'

	cache[1599440400] = 100
	cache[1599444000] = 101
	cache[1599508800] = 102
	*/

	/* Starting the HTTP server for external microservice */
	http.HandleFunc ("/", func (w http.ResponseWriter, req *http.Request) {

		// Headers (HTTP only)
	    w.Header ().Set ("Content-Type", "application/json")

	    // From/to (untrimmed)
	    from := req.URL.Query ().Get ("from")
	    to := req.URL.Query ().Get ("to")

	    // Response
        w.Write (common.QueryProcess (from, to, _params, func (from, to int64, response *common.RESTResponse) {

        	// Request's identification (now it's just for logging)
			puid := strconv.Itoa (int (time.Now ().UnixNano ()))

			_from := common.TimeToStr (from)
			_to := common.TimeToStr (to)

			if _params.Verbose {
				log.Printf ("[%s] Received a request from '%s' to '%s'", puid, _from, _to)
			}

        	// Splitting query to subqueries
        	subqueries := []*Query {}

        	// Mutex read on
        	cache_mutex.RLock ()

        	// Fresh cache
        	if len (cache) == 0 {

        		if _params.Verbose {
					log.Printf ("[%s] Fresh cache", puid)
				}

        		subqueries = append (subqueries, &Query {
        			From: from,
        			To: to,
        		})
        	} else {

        		/* Splitting */
        		if _params.Verbose {
					log.Printf ("[%s] Splitting into subqueries", puid)
				}

				// Updating the cache
				q := Query {}

				// Current timestamp is always updated
				c_ts := time.Now ().Unix ()
				c_ts /= _params.Timestep
				c_ts *= _params.Timestep

				for ts := from; ts <= to; ts += _params.Timestep {
					if _params.Debug {
						log.Printf ("[%s] [DEBUG] '%s' %d", puid, common.TimeToStr (ts), ts)
					}

					_, ex := cache[ts]
					if !ex || ts == c_ts {

						if _params.Debug {
							log.Printf ("[%s] [DEBUG]   - not in cache", puid)
						}

						if q.From == 0 { // first occurence
							
							q.From = ts
							q.To = ts
						} else {

							if ts == q.To + _params.Timestep { // if it's just next step and it's not in cache too
								
								// Continued
								q.To = ts

							} else {

								// Splitted
								// Adding it
								subqueries = append (subqueries, &Query {
				        			From: q.From,
				        			To: q.To,
				        		})

				        		// Resetting
				        		q.From = ts
				        		q.To = ts
							}
						}
					} else {
						if _params.Debug {
							log.Printf ("[%s] [DEBUG]   - in cache (passing)", puid)
						}
					}
				}

				// And the last one
				if q.From > 0 && q.To > 0 {
					subqueries = append (subqueries, &Query {
	        			From: q.From,
	        			To: q.To,
	        		})
	        	}
        	}

        	if _params.Verbose {
				log.Printf ("[%s] Total %d subqueries", puid, len (subqueries))
			}

        	// Sending queries
        	for _, q := range subqueries {
        		q.Response = make (chan *common.RESTResponse, 1)
        		q.Sent = time.Now ().Unix ()

        		queue <- q // blocking
        	}

        	// Waiting (endlessly) @TODO check for client gone = ?
        	for {
        		count := 0
        		for _, q := range subqueries {
        			if len (q.Response) == 1 {
        				count ++
        			} else {
        				
        				if _params.RepeatTimeoutS > 0 && time.Now ().Unix () - q.Sent >= _params.RepeatTimeoutS {

        					// Last disconnect time
        					ld_mutex.RLock ()
        					_ld := ld
        					ld_mutex.RUnlock ()

        					if _ld > q.Sent {

	        					// Re-sending only if there was a disconnect during query processing
	        					q.Sent = time.Now ().Unix ()
	        					queue <- q // blocking
	        				}
        				}
        			}
        		}

        		if count == len (subqueries) {
        			break
        		}

        		time.Sleep (time.Millisecond * 10)
        	}

        	// Mutex read off
        	cache_mutex.RUnlock ()

        	// Mutex write on
        	cache_mutex.Lock ()

        	// Reading response and updating the cache
        	for _, q := range subqueries { // they are not in the pool at the moment
        		resp := <- q.Response
        		
        		aggr_eth := resp.Data.(*map[int64]float64)

        		for ts := q.From; ts <= q.To; ts += _params.Timestep {
        			v := 0.0
        			
        			if _, ex := (*aggr_eth)[ts]; ex {
        				v = (*aggr_eth)[ts]
        			} 
        			
        			cache[ts] = v
        		}
        	}

        	// Mutex write off
        	cache_mutex.Unlock ()

        	// Sending query
        	// Building data
        	slice := []*ResponseItem {}

        	for ts := from; ts <= to; ts += _params.Timestep {
        		if _, ex := cache[ts]; ex {
					slice = append (slice, &ResponseItem {
						TimeS: ts,
						Value: cache[ts],
					})
				} // else @TODO very rare error				
			}

			// Sorting
			sort.Slice (slice, func(i, j int) bool {
				return slice[i].TimeS < slice[j].TimeS
			})

			// Sending
        	response.Status = true
            response.Data = slice

            if _params.Verbose {
				log.Printf ("[%s] Response sent", puid)
			}
        }))
    })

    log.Printf ("Server started at '%s'", _params.SolutionIPPort)

    go func () {
    	for {

    		log.Printf ("--- Solution status ---")

    		// Cache len
    		cache_mutex.RLock ()
    		_cl := len (cache)
    		cache_mutex.RUnlock ()

    		// Last disconnect
    		ld_mutex.RLock ()
			_ld := ld
			ld_mutex.RUnlock ()

			// Pool threads busy
			pool_mutex.RLock ()
			_pl := len (pool)
			pool_mutex.RUnlock ()

			log.Printf ("Cache: %d", _cl)
			log.Printf ("Pool: %d", _pl)
			log.Printf ("Last disconnect: %s", common.TimeToStr (_ld))

    		log.Printf ("-------------------")

    		time.Sleep (time.Minute)
    	}
    }()

    http.ListenAndServe (_params.SolutionIPPort, nil)
}