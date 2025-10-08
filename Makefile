test:
	go test ./... -race -cover -count 1

update-deps:
	go get -u ./...
	go mod tidy

fix:
	go fix ./...
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...


.PHONY: test update-deps fix
