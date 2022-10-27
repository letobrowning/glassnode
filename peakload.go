package main

import (
	"os"
	"log"
	"fmt"
	"time"
	"strconv"
	"net/http"
	"io/ioutil"
	"encoding/json"
	
	"database/sql"
	_ "github.com/lib/pq"

	"glassnode/lib/params"
	"glassnode/lib/common"

	// "github.com/davecgh/go-spew/spew"
)

func main () {

	/* Params (env) */
	_params := params.Get ()

	/* Command line */
	threads := 5

	if len (os.Args) == 2 {
		threads, _ = strconv.Atoi (os.Args[1])
	}

	log.Printf ("Threads: %d", threads)
	
	/* DB */
	connStr := fmt.Sprintf ("host=%s port=%d user=%s "+
        "password=%s dbname=%s sslmode=disable",
            _params.Host, _params.Port, _params.User, _params.Password, _params.DBName)

	pdb, err := sql.Open ("postgres", connStr)
    if err != nil {
        panic (err)
    }

    defer pdb.Close ()

    /* First and last time */
    GetTime := func (sorting string) int64 {
    	row := pdb.QueryRow (fmt.Sprintf ("SELECT block_time FROM transactions ORDER BY block_time %s LIMIT 1", sorting))

    	var block_time string
    	row.Scan (&block_time)

    	t, _ := common.StrToTime (block_time)
    	return t
    }

    t_from := GetTime ("ASC")
    t_to := GetTime ("DESC")
   
    log.Printf ("Transactions from '%s' to '%s'", common.TimeToStr (t_from), common.TimeToStr (t_to))

    /* Start testing */
    requests := make (chan bool, 100000)
    results := make (chan bool, 100000)

    for i := 0; i < threads; i ++ {
    	go func () {
    		for {
    			r := true

    			requests <- true

    			// Query
    			from := common.Rnd (t_from, t_to - _params.Timestep)
    			_to := from + _params.MaxQueryIntervalH * 48
    			if _to > t_to {
    				_to = t_to
    			}
    			to := common.Rnd (from, _to)

    			resp, err := http.Get (fmt.Sprintf ("http://%s/?from=%d&to=%d", _params.SolutionIPPort, from, to))

    			if err != nil {
			    	r = false
			    } else {

			    	// Response
			    	response, err := ioutil.ReadAll (resp.Body)
				    if err != nil {
				    	r = false
				    }

				    resp.Body.Close ()

				    // Parsing
				    rest_response := common.RESTResponse {}
				    err = json.Unmarshal (response, &rest_response)

				    r = rest_response.Status // && len (rest_response.Data.([]interface {})) == int (math.Ceil (float64 (to - from) / float64 (_params.Timestep))) @TODO gaps = ?
			    }
			    
			    results <- r
    		}
    	}()
    }

    var ok, req, all, prev float64 = 0, 0, 0, 0
    
    prev_t := time.Now ().Unix ()
    start_t := prev_t

    for range requests {

    	req ++

    	for {
    		if len (results) == 0 {
    			break
    		}

    		all ++

    		if <- results {
    			ok ++
    		}
    	}
		
		curr_t := time.Now ().Unix ()
		if curr_t - prev_t >= 10 {

			// Stat
	    	log.Printf ("%.0f/%.0f (%.2f%%) correct of total %.2f requests, speed: %.2f/sec, avg. speed: %.2f/sec", 
	    		ok, all, 100 * (ok/all), req, (all - prev) / 10, all / float64 (curr_t - start_t))
	    	
	    	prev = all
	    	prev_t = curr_t
		}
    }
}