build:
	go build ./cmd/courier

test:
	go test -p=1 -coverprofile=coverage.text -covermode=atomic ./...

coverage-report:
	go tool cover -html=coverage.text

test-cover:
	make test
	make coverage-report

total-coverage:
	go tool cover -func coverage.text | grep total | awk '{print $3}'

test-cover-total:
	make test
	make total-coverage
