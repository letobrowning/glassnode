package main

import (
	"os"
	"log"
	"fmt"
	"strings"
	"strconv"
	
	"database/sql"
	_ "github.com/lib/pq"

	"glassnode/lib/params"
	"glassnode/lib/common"

	// "github.com/davecgh/go-spew/spew"
)

type Transaction struct {
	Txid string
	Block_height int64
	Block_hash string
	Block_time string
	From string
	To string
	Value int64
	Gas_provided int64
	Gas_used int64
	Gas_price int64
	Status string
}

const (
	pack_size = 5000
)

func main () {

	/* Params (env) */
	_params := params.Get ()

	/* Command line */
	if len (os.Args) < 4 {
		fmt.Printf ("Params:\n")
		fmt.Printf (" - table name\n")
		fmt.Printf (" - from (YYYY-MM-DD)\n")
		fmt.Printf (" - to (YYYY-MM-DD)\n")
		return
	}

	tablename := os.Args[1]
	from, _ := common.StrToTime (os.Args[2] + " 00:00:00")
	to, _ := common.StrToTime (os.Args[3] + " 00:00:00")

	log.Printf ("Table name: %s", tablename)
	log.Printf ("From: %s", common.TimeToStr (from))
	log.Printf ("To: %s", common.TimeToStr (to))
	
	/* DB */
	connStr := fmt.Sprintf ("host=%s port=%d user=%s "+
        "password=%s dbname=%s sslmode=disable",
            _params.Host, _params.Port, _params.User, _params.Password, _params.DBName)

	pdb, err := sql.Open ("postgres", connStr)
    if err != nil {
        panic (err)
    }

    defer pdb.Close ()

    /* Table list */
    rows, _ := pdb.Query ("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
    tables_ex := make (map[string]bool)

	for rows.Next () {
		var table_name string
		rows.Scan (&table_name)

		if err != nil {
			continue
		}

		tables_ex[table_name] = true
	}

	rows.Close ()

    if _, ex := tables_ex[tablename]; ex {
    	log.Fatalln ("Table already exists")
    }

    /* Creating table */
	_, err = pdb.Query (`CREATE TABLE IF NOT EXISTS ` + tablename + ` (
	    txid text not null, 
		block_height bigint not null, 
		block_hash text not null, 
		block_time timestamp without time zone not null, 
		"from" text not null, 
		"to" text not null, 
		value numeric not null, 
		gas_provided numeric not null, 
		gas_used numeric not null, 
		gas_price numeric not null, 
		status text not null
	);`)
	if err != nil {
		panic (err)
	}

	/* Creating indexes */
	_, err = pdb.Query (`CREATE INDEX ` + tablename + `_from ON ` + tablename + `
	(
	    "from"
	);`)
	if err != nil {
		panic (err)
	}

	_, err = pdb.Query (`CREATE INDEX ` + tablename + `_to ON ` + tablename + `
	(
	    "to"
	);`)
	if err != nil {
		panic (err)
	}

	/* Loading all transactions */
	transactions := []*Transaction {}
	rows, _ = pdb.Query (`SELECT txid,block_height,block_hash,block_time,"from","to",value,gas_provided,gas_used,gas_price,status FROM transactions`)

	for rows.Next () {
		tx := Transaction {}
		rows.Scan (&tx.Txid, &tx.Block_height, &tx.Block_hash, &tx.Block_time, &tx.From, &tx.To, &tx.Value, &tx.Gas_provided, &tx.Gas_used, &tx.Gas_price, &tx.Status)

		if err != nil {
			continue
		}

		transactions = append (transactions, &tx)
	}

	rows.Close ()
	log.Printf ("Loaded %d transactions", len (transactions))

    /* Bulk inserting */
    step := int64 (3600 * 24)
    
    count := int ((to - from) / step)
    idx := 0

    for d := from; d < to; d += step {
    	t_from := d
    	t_to := d + step

    	idx ++
    	log.Printf ("[%d/%d] Inserting from '%s' to '%s'", idx, count, common.TimeToStr (t_from), common.TimeToStr (t_to))

    	// Preparing
    	for i := 0; i < len (transactions); i += pack_size {
    		log.Printf ("[%d/%d]   - pack %d..%d", idx, count, i, i + pack_size -1)

    		vals := []interface {}{}
	    	cnt := 0

	    	j := i + pack_size
	    	if j > len (transactions) {
	    		j = len (transactions)
	    	}
	    	
			for _, tx := range transactions[i:j] {
				t := common.TimeToStr (common.Rnd (t_from, t_to))
			    vals = append (vals, tx.Txid, tx.Block_height, tx.Block_hash, t, tx.From, tx.To, tx.Value, tx.Gas_provided, tx.Gas_used, tx.Gas_price, tx.Status)
			    cnt ++
			}

			// Executing
			sql_str := `INSERT INTO ` + tablename + ` (txid,block_height,block_hash,block_time,"from","to",value,gas_provided,gas_used,gas_price,status) VALUES `
			addon := []string {}

			n := 0
			for j := 0; j < cnt; j ++ {
				line := []string {}

				for k := 0; k < 11; k ++ {
					n ++
					line = append (line, "$" + strconv.Itoa (n))
				}

				addon = append (addon, "(" + strings.Join (line, ", ") + ")")
			}

			sql_str += strings.Join (addon, ", ")

			// Prepare and execute the statement
			stmt, _ := pdb.Prepare (sql_str)
			_, err := stmt.Exec (vals...)

			if err == nil {
				log.Printf ("[%d/%d]     - OK", idx, count)
			} else {
				panic (err)
			}
    	}
    }

    // @TODO contracts = ?
}