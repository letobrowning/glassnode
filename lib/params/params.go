package params

import (
	"github.com/caarlos0/env"
	"github.com/davecgh/go-spew/spew"
)

type Params struct {
	Host string `env:"Host" envDefault:"172.20.0.5"`
	Port int `env:"Port" envDefault:"5432"`
	User string `env:"User" envDefault:"test"`
	Password string `env:"Password" envDefault:"test"`
	DBName string `env:"DBName" envDefault:"eth"`
	TableName string `env:"TableName" envDefault:"transactions"`

	Verbose bool `env:"Verbose" envDefault:"true"`
	Debug bool `env:"Debug" envDefault:"false"`
	Timestep int64 `env:"Timestep" envDefault:"3600"`
	MaxQueryIntervalH int64 `env:"MaxQueryIntervalH" envDefault:"48"`

	PoolParallel int `env:"PoolParallel" envDefault:"10"`
	PoolTimeoutM int `env:"PoolTimeoutM" envDefault:"30"`
	
	RepeatTimeoutS int64 `env:"RepeatTimeoutS" envDefault:"30"`

	SolutionParallel int `env:"SolutionParallel" envDefault:"10"`

	PullerIPPort string `env:"PullerIPPort" envDefault:"172.20.0.6:7000"`
	SolutionIPPort string `env:"SolutionIPPort" envDefault:"0.0.0.0:7001"`
}

func Get () *Params {
	params := Params {}

	// Parsing
	env.Parse (&params)

	spew.Dump (params)
	return &params
}