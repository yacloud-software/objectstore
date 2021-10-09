.PHONY:	client server
all:	client server
client:
	cd client && go install objectstore-client.go `ls -1 *.go|grep -v objectstore-client.go`
server:
	cd server && go install objectstore-server.go `ls -1 *.go|grep -v objectstore-server.go`
