# kectl (Kubernetes Etcd Control)

Control Kubernetes objects from Etcd, used to prove concepts.

This project was inspired by [jpbetz/auger](https://github.com/jpbetz/auger) and [etcd-io/auger](https://github.com/etcd-io/auger)

## Usage

### Preparation cluster

Create a cluster and expose etcd port, to facilitate the creation of a cluster using kwokctl, this can be any other cluster

``` bash
# brew install kwok
kwokctl create cluster --etcd-port 2379
```

### Get a single resource

Get the kubernetes.default service

``` bash
kectl get services -n default kubernetes
```

### Get the all of the resource

``` bash
kectl get leases -n kube-system
```

### Get the all of the etcd

``` bash
kectl get
``` 

### Modify immutable data

``` bash
# change the creation time to very long ago
kectl get services -n default kubernetes | sed 's/creationTimestamp: .*/creationTimestamp: "2006-01-02T15:04:05Z"/' | kectl put --path -
kubectl get services -n default kubernetes
```

> Maybe patch subcommands can be added in the future

### Delete data

``` bash
kectl delete services -n default kubernetes
```
