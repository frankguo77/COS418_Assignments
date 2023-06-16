package raft

import "log"

// Debugging
const Debug = 6

func DPrintf(format string, a ...interface{}) (n int, err error) {
	DPrintf1(1, format, a...)

	return
}

func DPrintf1(lv int, format string, a ...interface{}) (n int, err error) {
	if lv == Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
