# ProjectPBFT

Implements a basic Key Value Store using the PBFT protocol. To fire up -

A simple example with 4 nodes - 
## Servers -
1) ./server -pbft 3001 -peer "127.0.0.1:3002" -peer "127.0.0.1:3003" -peer "127.0.0.1:3004" -port 3000 -byzantine true
2) ./server -pbft 3002 -peer "127.0.0.1:3001" -peer "127.0.0.1:3003" -peer "127.0.0.1:3004" -port 3006
3) ./server -pbft 3003 -peer "127.0.0.1:3002" -peer "127.0.0.1:3001" -peer "127.0.0.1:3004" -port 3007
4) ./server -pbft 3004 -peer "127.0.0.1:3002" -peer "127.0.0.1:3003" -peer "127.0.0.1:3001" -port 3008
## Proxy Client - 
1) ./client -primary "127.0.0.1:3001"
## CURL Request -
1) curl -X POST -F "req=set" -F "key=a" -F "value=b" http://127.0.0.1:8080
2) curl -X POST -F "req=get" -F "key=a" http://127.0.0.1:8080
