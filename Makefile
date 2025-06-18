dev:
	@go build -ldflags="-s -w" -o _dist/pantopic && cd cmd/standalone && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pantopic

wasm:
	@GOOS=wasip1 GOARCH=wasm tinygo build -buildmode=wasi-legacy -opt=2 -gc=leaking -scheduler=none -o test.wasm lmdb/test/module.go

wasm-prod:
	@GOOS=wasip1 GOARCH=wasm tinygo build -buildmode=wasi-legacy -no-debug -opt=2 -gc=leaking -scheduler=none -o test.prod.wasm lmdb/test/module.go

test:
	@go test

integration:
	@go test ./...

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc . --exclude-dir=_example,_dist,internal,cmd --exclude-ext=pb.go
