setup:
	go get -u github.com/golang/dep/cmd/dep

install:
	dep ensure

test:
	go vet ./...
	go test -race ./...