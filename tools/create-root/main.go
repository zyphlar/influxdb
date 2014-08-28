package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/coordinator"
)

func main() {
	addr := flag.String("address", "", "The raft address of any of the nodes")
	user := flag.String("username", "", "The username to be created")
	pass := flag.String("password", "", "The password of the new user")
	flag.Parse()

	if addr == nil || user == nil || pass == nil {
		fmt.Fprintf(os.Stderr, "Invalid arguments. Run `%s -h` for more information\n", os.Args[0])
		os.Exit(1)
	}

	hash, err := cluster.HashPassword(*pass)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while hashing password: %s\n", err)
		os.Exit(2)
	}

	u := cluster.ClusterAdmin{
		cluster.CommonUser{
			Name:     *user,
			CacheKey: *user,
			Hash:     string(hash),
		},
	}
	cmd := coordinator.NewSaveClusterAdminCommand(&u)
	_, err = coordinator.SendCommandToServer(*addr, cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot run command: %s\n", err)
		os.Exit(3)
	}
	fmt.Printf("Operationsucceeded\n")
}
