resource "kubernetes_role_v1" "pod_operator" {
  metadata {
    name      = var.role_name
    namespace = var.namespace
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = var.pod_verbs
  }

  rule {
    api_groups = [""]
    resources  = ["pods/log"]
    verbs      = var.pod_log_verbs
  }

  rule {
    api_groups = [""]
    resources  = ["pods/exec"]
    verbs      = var.pod_exec_verbs
  }
}

resource "kubernetes_role_binding_v1" "pod_operator" {
  metadata {
    name      = var.role_binding_name
    namespace = var.namespace
  }

  subject {
    kind      = "Group"
    name      = var.kubernetes_group
    api_group = "rbac.authorization.k8s.io"
  }

  role_ref {
    kind      = "Role"
    name      = kubernetes_role_v1.pod_operator.metadata[0].name
    api_group = "rbac.authorization.k8s.io"
  }
}
