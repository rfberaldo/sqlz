test:
	go test ./... -race -cover -count 1

update-deps:
	go get -u ./...
	go mod tidy

fix:
	go fix ./...
	go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...

docs:
	cd docs && npm install && npm run dev

.PHONY: test update-deps fix docs
