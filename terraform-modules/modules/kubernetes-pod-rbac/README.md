# kubernetes-pod-rbac

Creates namespaced Kubernetes RBAC for human pod operations.

By default, the bound group can:

- list, watch, get, and delete pods
- read pod logs
- create `pods/exec` sessions

Use this with an EKS access entry that maps an IAM Identity Center role into the same Kubernetes group.
