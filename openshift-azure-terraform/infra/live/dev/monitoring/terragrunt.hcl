include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "../../../modules/monitoring"
}

dependency "aks" {
  config_path = "../aks"
}

inputs = {
  kube_config = dependency.aks.outputs.kube_config
}
