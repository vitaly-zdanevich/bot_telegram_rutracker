output "function_url" {
  description = "Public Lambda function URL for Telegram setWebhook."
  value       = aws_lambda_function_url.bot.function_url
}

output "lambda_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.bot.function_name
}

