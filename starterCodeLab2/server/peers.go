package main

import "strings"

// Define a type that can be used by the flag library to collect an array of strings.
type arrayPeers []string
type clientPeer string

// Convert array to a string
func (a *arrayPeers) String() string {
	return strings.Join(*a, ",")
}

// Add a string
func (a *arrayPeers) Set(v string) error {
	*a = append(*a, v)
	return nil
}

// func (a clientPeer) Set(v string) error {
// 	*a = v
// 	return nil
// }
