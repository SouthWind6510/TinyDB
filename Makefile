build:
	go mod download
	go build -o tinydb-server ./cmd/