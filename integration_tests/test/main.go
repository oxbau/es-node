package main

import "os"

func main() {
	if len(os.Args) == 0 {
		return
	} else {
		os.Exit(1)
	}
}
