
.PHONY: generate
generate: \
	pkg/wellknown/resources.go \
	pkg/scheme/scheme.go \
	pkg/old/api \
	pkg/old/scheme/scheme.go

pkg/scheme/scheme.go: ./hack/gen_scheme.sh go.mod
	go mod vendor
	-rm ./pkg/scheme/scheme.go
	./hack/gen_scheme.sh > ./pkg/scheme/scheme.go

pkg/old/api:
	./hack/clone_old_apis.sh 35

pkg/old/scheme/scheme.go: ./hack/gen_old_scheme.sh pkg/old/apis go.mod
	-rm ./pkg/old/scheme/scheme.go
	./hack/gen_old_scheme.sh > ./pkg/old/scheme/scheme.go

pkg/wellknown/resources.go: ./hack/gen_wellknown_resources go.mod
	-kwokctl delete cluster --name kectl-wellknown
	KWOK_KUBE_VERSION=1.35.0 kwokctl create cluster --name kectl-wellknown \
		--kube-apiserver-insecure-port 8080 \
		--runtime binary \
		--disable-kube-controller-manager \
		--disable-kube-scheduler \
		--kubeconfig ./kectl-wellknown.kubeconfig
	-rm pkg/wellknown/resources.go
	go run ./hack/gen_wellknown_resources ./kectl-wellknown.kubeconfig > pkg/wellknown/resources.go
	kwokctl delete cluster --name kectl-wellknown \
		--kubeconfig ./kectl-wellknown.kubeconfig
	rm ./kectl-wellknown.kubeconfig

bin/kectl:
	go build -o bin/kectl ./cmd/kectl
