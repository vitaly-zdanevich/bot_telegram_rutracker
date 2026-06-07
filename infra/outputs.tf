output "function_url" {
  description = "Public Lambda function URL for Telegram setWebhook."
  value       = aws_lambda_function_url.bot.function_url
}

output "lambda_name" {
  description = "Webhook Lambda function name."
  value       = aws_lambda_function.bot.function_name
}

output "worker_lambda_name" {
  description = "Worker Lambda function name."
  value       = aws_lambda_function.worker.function_name
}
