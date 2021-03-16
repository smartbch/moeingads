package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/smartbch/MoeingADS/datatree"
)

func printUsage() {
	fmt.Printf("Usage: %s genacc <number-of-accounts>\n", os.Args[0])
	fmt.Printf("Usage: %s checkacc <number-of-accounts>\n", os.Args[0])
	fmt.Printf("Usage: %s gentx <number-of-accounts> <number-of-blocks>\n", os.Args[0])
	fmt.Printf("Usage: %s runtx <number-of-blocks>\n", os.Args[0])
}

func main() {
	if datatree.BufferSize < 512*1024 {
		panic("BufferSize too small")
	}
	if len(os.Args) < 3 {
		printUsage()
		return
	}
	if os.Args[1] != "genacc" && os.Args[1] != "checkacc" && os.Args[1] != "gentx" && os.Args[1] != "runtx" {
		fmt.Printf("Invalid sub-command: %s\n", os.Args[1])
		printUsage()
		return
	}
	if os.Args[1] == "genacc" {
		if len(os.Args) != 3 {
			printUsage()
			return
		}
		randFilename := os.Getenv("RANDFILE")
		if len(randFilename) == 0 {
			fmt.Printf("No RANDFILE specified. Exiting...\n")
			return
		}
		numAccounts, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		RunGenerateAccounts(numAccounts, randFilename, "shortkey.json")
	}
	if os.Args[1] == "checkacc" {
		if len(os.Args) != 3 {
			printUsage()
			return
		}
		randFilename := os.Getenv("RANDFILE")
		if len(randFilename) == 0 {
			fmt.Printf("No RANDFILE specified. Exiting...")
			return
		}
		numAccounts, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		RunCheckAccounts(numAccounts, randFilename)
	}
	if os.Args[1] == "gentx" {
		if len(os.Args) != 4 {
			printUsage()
			return
		}
		randFilename := os.Getenv("RANDFILE")
		if len(randFilename) == 0 {
			fmt.Printf("No RANDFILE specified. Exiting...")
			return
		}
		numAccounts, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		numBlock, err := strconv.Atoi(os.Args[3])
		if err != nil {
			panic(err)
		}
		RunGenerateTxFile(numBlock*NumEpochInBlock, uint64(numAccounts), "shortkey.json", randFilename, "tx.dat")
	}
	if os.Args[1] == "runtx" {
		if len(os.Args) != 3 {
			printUsage()
			return
		}
		numBlock, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
		RunTx(numBlock, "tx.dat")
	}
}
