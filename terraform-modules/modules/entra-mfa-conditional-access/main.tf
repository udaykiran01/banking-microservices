data "azuread_service_principal" "aws_identity_center" {
  display_name = var.target_application_display_name
}

resource "azuread_conditional_access_policy" "this" {
  display_name = var.policy_name
  state        = var.state

  conditions {
    client_app_types = var.client_app_types

    applications {
      included_applications = [data.azuread_service_principal.aws_identity_center.client_id]
    }

    users {
      included_users  = var.included_users
      excluded_users  = var.excluded_users
      included_groups = var.included_groups
      excluded_groups = var.excluded_groups
    }
  }

  grant_controls {
    operator          = "OR"
    built_in_controls = ["mfa"]
  }
}
