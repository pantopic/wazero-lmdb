dev:
	@go build -ldflags="-s -w" -o _dist/pantopic && cd cmd/standalone && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pantopic

wasm:
	@tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o host/test.wasm test/module.go

wasm-prod:
	@tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=conservative -scheduler=none -o host/test.prod.wasm -no-debug test/module.go

test:
	@go test ./host

cover:
	@mkdir -p _dist
	@go test ./host -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc ./host --exclude-dir=_example,_dist,internal,cmd --exclude-ext=pb.go

.PHONY: all test clean
