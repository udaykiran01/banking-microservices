data "aws_ssoadmin_instances" "this" {}

locals {
  identity_store_id = coalesce(var.identity_store_id, one(data.aws_ssoadmin_instances.this.identity_store_ids))
}

data "aws_identitystore_group" "this" {
  identity_store_id = local.identity_store_id

  alternate_identifier {
    unique_attribute {
      attribute_path  = "DisplayName"
      attribute_value = var.display_name
    }
  }
}
