locals {
  tags = { project = "meterstream", managed_by = "terraform" }
  app_env = {
    BROKER     = "redis"
    STORE      = "timescale"
    PARTITIONS = tostring(var.partitions)
  }
}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "azurerm_resource_group" "rg" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_log_analytics_workspace" "logs" {
  name                = "${var.prefix}-logs"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

# --- Data stores -------------------------------------------------------------------------------

resource "azurerm_redis_cache" "redis" {
  name                 = "${var.prefix}-redis-${random_string.suffix.result}"
  location             = azurerm_resource_group.rg.location
  resource_group_name  = azurerm_resource_group.rg.name
  capacity             = 0
  family               = "C"
  sku_name             = "Basic"
  minimum_tls_version  = "1.2"
  non_ssl_port_enabled = false
  tags                 = local.tags
}

resource "azurerm_postgresql_flexible_server" "db" {
  name                          = "${var.prefix}-pg-${random_string.suffix.result}"
  location                      = azurerm_resource_group.rg.location
  resource_group_name           = azurerm_resource_group.rg.name
  version                       = "16"
  sku_name                      = var.db_sku
  storage_mb                    = 32768
  administrator_login           = "meter"
  administrator_password        = random_password.db.result
  public_network_access_enabled = true
  zone                          = "1"
  tags                          = local.tags
}

# TimescaleDB must be allow-listed and preloaded on Azure Flexible Server.
resource "azurerm_postgresql_flexible_server_configuration" "extensions" {
  name      = "azure.extensions"
  server_id = azurerm_postgresql_flexible_server.db.id
  value     = "TIMESCALEDB"
}

resource "azurerm_postgresql_flexible_server_configuration" "preload" {
  name      = "shared_preload_libraries"
  server_id = azurerm_postgresql_flexible_server.db.id
  value     = "timescaledb"
}

resource "azurerm_postgresql_flexible_server_database" "meter" {
  name      = "meter"
  server_id = azurerm_postgresql_flexible_server.db.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# Allows traffic from Azure services (Container Apps). For production, use VNet integration instead.
resource "azurerm_postgresql_flexible_server_firewall_rule" "azure" {
  name             = "allow-azure-services"
  server_id        = azurerm_postgresql_flexible_server.db.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

locals {
  redis_url    = "rediss://:${azurerm_redis_cache.redis.primary_access_key}@${azurerm_redis_cache.redis.hostname}:${azurerm_redis_cache.redis.ssl_port}/0"
  database_url = "postgresql://meter:${random_password.db.result}@${azurerm_postgresql_flexible_server.db.fqdn}:5432/meter?sslmode=require"
}

# --- Compute: Azure Container Apps ------------------------------------------------------------

resource "azurerm_container_app_environment" "env" {
  name                       = "${var.prefix}-env"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.logs.id
  tags                       = local.tags
}

resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"
  tags                         = local.tags

  secret {
    name  = "database-url"
    value = local.database_url
  }
  secret {
    name  = "redis-url"
    value = local.redis_url
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  template {
    min_replicas = 1
    max_replicas = 3
    container {
      name   = "api"
      image  = var.image
      args   = ["api", "--port", "8000"]
      cpu    = 0.5
      memory = "1Gi"
      dynamic "env" {
        for_each = local.app_env
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name        = "DATABASE_URL"
        secret_name = "database-url"
      }
      env {
        name        = "REDIS_URL"
        secret_name = "redis-url"
      }
      liveness_probe {
        transport = "HTTP"
        port      = 8000
        path      = "/health"
      }
    }
    http_scale_rule {
      name                = "http"
      concurrent_requests = "50"
    }
  }
}

# One container app per partition: exactly one replica owns each partition, so per-meter
# detector state is never split. Scale out by raising var.partitions.
resource "azurerm_container_app" "worker" {
  count                        = var.partitions
  name                         = "${var.prefix}-worker-${count.index}"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"
  tags                         = local.tags

  secret {
    name  = "database-url"
    value = local.database_url
  }
  secret {
    name  = "redis-url"
    value = local.redis_url
  }

  template {
    min_replicas = 1
    max_replicas = 1
    container {
      name   = "worker"
      image  = var.image
      args   = ["worker", "--consumer", "worker-${count.index}"]
      cpu    = 0.5
      memory = "1Gi"
      dynamic "env" {
        for_each = merge(local.app_env, { PARTITION = tostring(count.index) })
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name        = "DATABASE_URL"
        secret_name = "database-url"
      }
      env {
        name        = "REDIS_URL"
        secret_name = "redis-url"
      }
    }
  }

  depends_on = [
    azurerm_postgresql_flexible_server_configuration.extensions,
    azurerm_postgresql_flexible_server_configuration.preload,
  ]
}

resource "azurerm_container_app" "simulator" {
  count                        = var.enable_simulator ? 1 : 0
  name                         = "${var.prefix}-simulator"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"
  tags                         = local.tags

  secret {
    name  = "redis-url"
    value = local.redis_url
  }

  template {
    min_replicas = 1
    max_replicas = 1
    container {
      name   = "simulator"
      image  = var.image
      args   = ["simulate", "--meters", tostring(var.simulated_meters), "--speed", "60", "--backfill-days", "2"]
      cpu    = 0.25
      memory = "0.5Gi"
      env {
        name  = "BROKER"
        value = "redis"
      }
      env {
        name  = "PARTITIONS"
        value = tostring(var.partitions)
      }
      env {
        name        = "REDIS_URL"
        secret_name = "redis-url"
      }
    }
  }
}
