variable "oci_region" {
  description = "Oracle Cloud region for the ARM VM."
  type        = string
}

variable "oci_tenancy_ocid" {
  description = "Oracle Cloud tenancy OCID."
  type        = string
  sensitive   = true
}

variable "oci_user_ocid" {
  description = "Oracle Cloud user OCID."
  type        = string
  sensitive   = true
}

variable "oci_fingerprint" {
  description = "Oracle Cloud API key fingerprint."
  type        = string
  sensitive   = true
}

variable "oci_private_key_path" {
  description = "Local path to the Oracle Cloud API private key."
  type        = string
  sensitive   = true
}

variable "oci_compartment_ocid" {
  description = "Oracle Cloud compartment OCID where the VM will be created."
  type        = string
  sensitive   = true
}

variable "availability_domain" {
  description = "Optional availability domain name. Empty uses the first AD in the region."
  type        = string
  default     = ""
}

variable "project_name" {
  description = "Oracle resource display-name prefix."
  type        = string
  default     = "telegram-rutracker-bot"
}

variable "ssh_public_key" {
  description = "Public SSH key installed into the VM."
  type        = string
  sensitive   = true
}

variable "ssh_ingress_cidr" {
  description = "CIDR allowed to SSH into the VM."
  type        = string
  default     = "0.0.0.0/0"
}

variable "vm_worker_ingress_cidr" {
  description = "CIDR allowed to reach the signed VM worker HTTP endpoint."
  type        = string
  default     = "0.0.0.0/0"
}

variable "torrent_ingress_cidr" {
  description = "CIDR allowed to reach the torrent TCP/uTP listen port."
  type        = string
  default     = "0.0.0.0/0"
}

variable "oracle_image_ocid" {
  description = "Optional ARM-compatible Oracle image OCID. Leave empty to auto-select the latest matching Ubuntu image."
  type        = string
  default     = ""
}

variable "oracle_image_operating_system" {
  description = "Operating system used when oracle_image_ocid is empty."
  type        = string
  default     = "Canonical Ubuntu"
}

variable "oracle_image_operating_system_version" {
  description = "Operating system version used when oracle_image_ocid is empty."
  type        = string
  default     = "24.04"
}

variable "oracle_shape" {
  description = "Oracle Always Free ARM shape."
  type        = string
  default     = "VM.Standard.A1.Flex"
}

variable "oracle_ocpus" {
  description = "OCPUs for the ARM VM. Always Free Ampere A1 accounts commonly allow up to 4."
  type        = number
  default     = 4
}

variable "oracle_memory_gb" {
  description = "Memory for the ARM VM in GB. Always Free Ampere A1 accounts commonly allow up to 24 GB."
  type        = number
  default     = 24
}

variable "oracle_boot_volume_gb" {
  description = "Boot volume size in GB."
  type        = number
  default     = 200
}

variable "vcn_cidr" {
  description = "VCN CIDR."
  type        = string
  default     = "10.42.0.0/16"
}

variable "subnet_cidr" {
  description = "Public subnet CIDR."
  type        = string
  default     = "10.42.1.0/24"
}

variable "telegram_bot_token" {
  description = "Telegram bot token."
  type        = string
  sensitive   = true
}

variable "telegram_webhook_secret" {
  description = "Unused in long-polling mode, but required by the shared app config."
  type        = string
  default     = "oracle-long-polling"
  sensitive   = true
}

variable "telegram_api_id" {
  description = "Telegram API ID for tdlib telegram-bot-api."
  type        = number
  sensitive   = true
}

variable "telegram_api_hash" {
  description = "Telegram API hash for tdlib telegram-bot-api."
  type        = string
  sensitive   = true
}

variable "vm_worker_secret" {
  description = "Shared HMAC secret for Lambda to VM worker dispatch."
  type        = string
  sensitive   = true
}

variable "allowed_telegram_user_ids" {
  description = "Comma-separated Telegram user IDs. Empty string makes the bot public."
  type        = string
  default     = ""
}

variable "rutracker_base_urls" {
  description = "Comma-separated RuTracker forum base URLs tried in order."
  type        = string
  default     = "https://rutracker.org/forum,https://rutracker.net/forum,https://rutracker.nl/forum"
}

variable "rutracker_username" {
  description = "Optional RuTracker username for authenticated search."
  type        = string
  default     = ""
  sensitive   = true
}

variable "rutracker_password" {
  description = "Optional RuTracker password for authenticated search."
  type        = string
  default     = ""
  sensitive   = true
}

variable "rutracker_cookie" {
  description = "Optional RuTracker cookie fallback."
  type        = string
  default     = ""
  sensitive   = true
}

variable "bot_repo_url" {
  description = "Git repository cloned on the VM to build the Rust poller."
  type        = string
  default     = "https://github.com/vitaly-zdanevich/bot_telegram_rutracker.git"
}

variable "bot_repo_ref" {
  description = "Git branch, tag, or commit checked out on the VM."
  type        = string
  default     = "main"
}

variable "max_file_mb" {
  description = "Configured upload limit for tdlib local Bot API mode."
  type        = number
  default     = 2000
}

variable "download_timeout_seconds" {
  description = "VM download time budget in seconds."
  type        = number
  default     = 86400
}

variable "download_margin_seconds" {
  description = "Final window before the bot stops waiting for more files."
  type        = number
  default     = 60
}

variable "torrent_peer_limit" {
  description = "Torrent peer limit."
  type        = number
  default     = 240
}

variable "torrent_listen_port" {
  description = "TCP and UDP port used by librqbit for incoming torrent peers."
  type        = number
  default     = 49152

  validation {
    condition     = var.torrent_listen_port >= 1024 && var.torrent_listen_port <= 65535
    error_message = "torrent_listen_port must be between 1024 and 65535."
  }
}

variable "seed_disk_reserve_mb" {
  description = "Optional extra free disk reserve retained after fitting a new seeded torrent."
  type        = number
  default     = 0
}
