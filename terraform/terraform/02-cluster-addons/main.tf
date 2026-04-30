resource "helm_release" "nginx_ingress" {
  name       = "nginx-ingress"
  namespace  = "ingress-nginx"

  repository = "https://kubernetes.github.io/ingress-nginx"
  chart      = "ingress-nginx"

  create_namespace = true
}


resource "helm_release" "monitoring" {
  name       = "monitoring"
  namespace  = "monitoring"

  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"

  create_namespace = true
}

resource "helm_release" "strimzi" {
  name       = "strimzi"
  namespace  = "kafka"

  repository = "https://strimzi.io/charts/"
  chart      = "strimzi-kafka-operator"

  create_namespace = true
}


resource "helm_release" "csi_driver" {
  name       = "csi-secrets-store"
  namespace  = "kube-system"

  repository = "https://kubernetes-sigs.github.io/secrets-store-csi-driver/charts"
  chart      = "secrets-store-csi-driver"
}

resource "helm_release" "azure_kv_provider" {
  name       = "azure-keyvault-provider"
  namespace  = "kube-system"

  repository = "https://azure.github.io/secrets-store-csi-driver-provider-azure/charts"
  chart      = "csi-secrets-store-provider-azure"
}



