package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

const logo = `
+---------------------------------------------+
|  _____        __ _            _____  ____   |
| |_   _|      / _| |          |  __ \|  _ \  |
|   | |  _ __ | |_| |_   ___  _| |  | | |_) | |
|   | | | '_ \|  _| | | | \ \/ / |  | |  _ <  |
|  _| |_| | | | | | | |_| |>  <| |__| | |_) | |
| |_____|_| |_|_| |_|\__,_/_/\_\_____/|____/  |
+---------------------------------------------+
`

// These variables are populated via the Go linker.
var (
	version string = "0.9"
	commit  string
)

func main() {
	log.SetFlags(0)

	// Shift binary name off argument list.
	args := os.Args[1:]

	// Retrieve command name as first argument.
	var cmd string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if cmd == "help" && len(args) > 1 {
		args[0], args[1] = args[1], "-h"
		cmd = args[0]
	}

	// Extract name from args.
	switch cmd {
	case "create-cluster":
		execCreateCluster(args[1:])
	case "join-cluster":
		execJoinCluster(args[1:])
	case "run":
		execRun(args[1:])
	case "":
		execRun(args)
	case "version":
		execVersion(args[1:])
	case "help":
		execHelp(args[1:])
	default:
		log.Fatalf(`influxd: unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", cmd)
	}
}

// execVersion runs the "version" command.
// Prints the commit SHA1 if set by the build process.
func execVersion(args []string) {
	s := fmt.Sprintf("InfluxDB v%s", version)
	if commit != "" {
		s += fmt.Sprintf(" (git: %s)", commit)
	}
	log.Print(s)
}

// execHelp runs the "help" command.
func execHelp(args []string) {
	fmt.Println(`
Configure and start an InfluxDB server.

Usage:

	influxd [[command] [arguments]]

The commands are:

    create-cluster       create a new node that other nodes can join to form a new cluster
    join-cluster         create a new node that will join an existing cluster
    run                  run node with existing configuration
    version              displays the InfluxDB version

"run" is the default command.

Use "influxd help [command]" for more information about a command.
`)
}

type Stopper interface {
	Stop()
}

type State struct {
	Mode string `json:"mode"`
}

// createStateIfNotExists returns the cluster state, from the file at path.
// If no file exists at path, the default state is created, and written to the path.
func createStateIfNotExists(path string) (*State, error) {
	// Read state from path.
	// If state doesn't exist then return a "local" state.
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return &State{Mode: "local"}, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	// Decode state from file and return.
	s := &State{}
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil, err
	}

	return s, nil
}
