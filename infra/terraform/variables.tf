variable "subscription_id" {
  type        = string
  description = "Azure subscription to deploy into."
}

variable "prefix" {
  type        = string
  default     = "meterstream"
  description = "Name prefix for all resources (lowercase letters and digits)."
}

variable "location" {
  type    = string
  default = "centralindia"
}

variable "image" {
  type        = string
  description = "Container image, e.g. ghcr.io/<user>/meterstream:<sha>."
}

variable "partitions" {
  type        = number
  default     = 2
  description = "Stream partitions = worker replicas (each worker owns one partition)."
}

variable "enable_simulator" {
  type        = bool
  default     = true
  description = "Run the fleet simulator as a data source (turn off when real meters publish)."
}

variable "simulated_meters" {
  type    = number
  default = 1000
}

variable "db_sku" {
  type    = string
  default = "B_Standard_B1ms" # burstable: fine for a demo, use GP_Standard_D2ds_v5+ for real load
}
