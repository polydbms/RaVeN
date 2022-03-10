variable "resource_group_name" {
    type        = string
    default     = "generated_rg"
}

variable "location" {
    type        = string
    default     = "eastus"
}

variable "vm_name" {
    type        = string
    default     = "myvm"
}

variable "public_key" {
    type        = string
}

variable "machine_size" {
    type        = string
    default     = "Standard_B2ms"
}

variable "extra_port" {
    type        = string
    default     = "8080"
}