
resource "helm_release" "banking_app" {
  name      = "banking-app"
  chart     = "../../../helm/banking-app"
  namespace = "banking-app"

  create_namespace = true

  values = [
    file("../../../helm/banking-app/values-aws.yaml")
  ]
}