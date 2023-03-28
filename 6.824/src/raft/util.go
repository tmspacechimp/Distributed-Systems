package raft

import "log"

type Role int

const (
	FOLLOWER Role = iota
	LEADER
	CANDIDATE
)

const NoVote = -1

const (
	NoHeartbeatsRandomRange = 150
	NoHeartbeatsAlarmTime   = 200
	HeartbeatSendingRate    = 80
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
