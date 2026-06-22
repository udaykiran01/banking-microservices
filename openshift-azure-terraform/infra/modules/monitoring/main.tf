provider "kubernetes" {
  config_path = null

  host                   = yamldecode(var.kube_config).clusters[0].cluster.server
  client_certificate     = base64decode(yamldecode(var.kube_config).users[0].user.client-certificate-data)
  client_key             = base64decode(yamldecode(var.kube_config).users[0].user.client-key-data)
  cluster_ca_certificate = base64decode(yamldecode(var.kube_config).clusters[0].cluster.certificate-authority-data)
}

provider "helm" {
  kubernetes = {
    host                   = yamldecode(var.kube_config).clusters[0].cluster.server
    client_certificate     = base64decode(yamldecode(var.kube_config).users[0].user.client-certificate-data)
    client_key             = base64decode(yamldecode(var.kube_config).users[0].user.client-key-data)
    cluster_ca_certificate = base64decode(yamldecode(var.kube_config).clusters[0].cluster.certificate-authority-data)
  }
}

resource "helm_release" "kube_prometheus_stack" {
  name             = "monitoring"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  namespace        = "monitoring"
  create_namespace = true

  values = [
    <<EOF
grafana:
  adminPassword: "admin123"

prometheus:
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false
    podMonitorSelectorNilUsesHelmValues: false

alertmanager:
  enabled: true
EOF
  ]
}