package common

import (
	"fmt"
	"time"
	"math"
	"strings"
	"strconv"
	"crypto/rand"
	"encoding/json"
	"encoding/binary"

	"glassnode/lib/params"
)

// @DEBUG
// {"uid":"1","from":"1599436800","to":"1599523200"}
type WSRequest struct {
	UID string `json:"uid"`
	From string `json:"from"`
	To string `json:"to"`
}

type RESTResponse struct {
	Status bool `json:"status"`
	Error string `json:"error"`
	Data interface {} `json:"data"`
}

type InternalMessage struct {
	UID string `json:"uid"`
	Map *map[int64]float64 `json:"map"`
}

// REST query processing
func QueryProcess (_from, _to string, _params *params.Params, callback func (from, to int64, response *RESTResponse)) []byte {

	// Response
    response := RESTResponse {
    	Status: false, // so it's false by default, but it's for code readability 
    }

    // From/to parameters with checking
    __ := func (s string) int64 {
    	i, e := strconv.Atoi (s)
    	if e != nil {
    		return 0
    	}

    	_i := int64 (i)

    	_i /= _params.Timestep
    	_i *= _params.Timestep

    	return _i
    }

    from := __ (_from)
    to   := __ (_to)

    if from < 0 || to <= 0 {
    	response.Error = "Incorrect parameter(s) format"
    } else {

    	if to - from >= _params.MaxQueryIntervalH * 3600 {
    		response.Error = fmt.Sprintf ("Time interval can't be more than %d hours", _params.MaxQueryIntervalH)
    	} else {

    		// External callback
    		callback (from, to, &response)
    	}
    }

    // Writing
	bin, err := json.Marshal (response)

	if err != nil {
		// Re-writing
		bin, _ = json.Marshal (&RESTResponse {
			Status: false,
			Error: "Internal data marshalling error",
		})
	}

	return bin
}

// Date and time methods
func StrToTime (s string) (int64, bool) {
	s = strings.Replace (strings.Replace (s, "T", " ", -1), "Z", "", -1)

	t, err := time.Parse (time.RFC3339, strings.Replace (s, " ", "T", 1) + ".000Z")
	if err != nil {
	    return 0, false
	}
	return t.Unix (), true
}

func TimeToStr (t int64) string {
	loc, _ := time.LoadLocation ("UTC")
	return strings.Replace (time.Unix (t, 0).In (loc).String (), " +0000 UTC", "", -1)
}

// Crypto-safe random generator
func Rnd (min, max int64) int64 {
	bytes := [8]byte{}
    _, err := rand.Read (bytes[:])
    if err != nil {
        panic (err)
    }
    var bytes_s []byte
    for i := 0; i < 8; i ++ {
    	bytes_s = append (bytes_s, bytes[i])
    }
    rnd_uint := binary.LittleEndian.Uint64 (bytes_s)
    rnd_norm := float64 (rnd_uint) / (math.MaxUint64 + 1)

    return min + int64 (float64 (max - min) * rnd_norm)
}