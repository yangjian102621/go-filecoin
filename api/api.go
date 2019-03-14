// Package api holds the interface definitions for the Filecoin api.
package api

// API is the user interface to a Filecoin node.
type API interface {
	Actor() Actor
	Address() Address
	Client() Client
	Daemon() Daemon
	Miner() Miner
	Mining() Mining
	Ping() Ping
	RetrievalClient() RetrievalClient
	Swarm() Swarm
}
