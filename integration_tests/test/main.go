package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("=======starting========")
	if len(os.Args) == 1 {
		fmt.Println("exit with null")
		return
	} else if len(os.Args) == 2 {
		fmt.Println("exit with 0")
		os.Exit(0)
	} else {
		fmt.Println("exit with 1")
		os.Exit(1)
	}
}
