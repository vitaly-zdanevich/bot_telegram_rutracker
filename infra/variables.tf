variable "aws_region" {
  description = "AWS region. eu-north-1 is the default to avoid Germany for RuTracker connectivity concerns."
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Lambda function and log group name."
  type        = string
  default     = "telegram-rutracker-bot"
}

variable "lambda_zip" {
  description = "Path to the Lambda deployment ZIP produced by scripts/build-lambda.sh."
  type        = string
  default     = "../build/lambda.zip"
}

variable "lambda_memory_size" {
  description = "Lambda memory in MB. This AWS account/region currently accepts 3008 MB as the maximum."
  type        = number
  default     = 3008

  validation {
    condition     = var.lambda_memory_size >= 128 && var.lambda_memory_size <= 10240
    error_message = "lambda_memory_size must be between 128 and 10240 MB."
  }
}

variable "telegram_bot_token" {
  description = "Telegram bot token."
  type        = string
  sensitive   = true
}

variable "telegram_webhook_secret" {
  description = "Secret token Telegram sends in X-Telegram-Bot-Api-Secret-Token."
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

variable "rutracker_cookie" {
  description = "Optional RuTracker cookie fallback. Prefer rutracker_username/rutracker_password for authenticated search."
  type        = string
  default     = ""
  sensitive   = true
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
