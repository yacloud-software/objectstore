.PHONY:	client server
all:	client server
client:
	cd client && go install ${LDFLAGS} objectstore-client.go `ls -1 *.go|grep -v objectstore-client.go`
server:
	cd server && go install ${LDFLAGS} objectstore-server.go `ls -1 *.go|grep -v objectstore-server.go`
