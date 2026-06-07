data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

locals {
  worker_function_name = "${var.project_name}-worker"

  webhook_environment_variables = {
    TELEGRAM_WEBHOOK_SECRET = var.telegram_webhook_secret
    WORKER_FUNCTION_NAME    = local.worker_function_name
    RUST_LOG                = "info"
  }

  worker_environment_variables = merge(
    {
      TELEGRAM_BOT_TOKEN             = var.telegram_bot_token
      TELEGRAM_WEBHOOK_SECRET        = var.telegram_webhook_secret
      RUTRACKER_BASE_URLS            = var.rutracker_base_urls
      SEARCH_LIMIT                   = "10"
      RUTRACKER_HTTP_TIMEOUT_SECONDS = "30"
      RUTRACKER_HTTP_MAX_ATTEMPTS    = "10"
      MAX_FILE_MB                    = "50"
      LAMBDA_TIMEOUT_SECONDS         = "900"
      DOWNLOAD_MARGIN_SECONDS        = "20"
      TORRENT_PEER_LIMIT             = "120"
      RUST_LOG                       = "info"
    },
    var.allowed_telegram_user_ids == "" ? {} : {
      ALLOWED_TELEGRAM_USER_IDS = var.allowed_telegram_user_ids
    },
    var.rutracker_cookie == "" ? {} : {
      RUTRACKER_COOKIE = var.rutracker_cookie
    },
    var.rutracker_username == "" ? {} : {
      RUTRACKER_USERNAME = var.rutracker_username
    },
    var.rutracker_password == "" ? {} : {
      RUTRACKER_PASSWORD = var.rutracker_password
    }
  )
}

resource "aws_iam_role" "lambda" {
  name               = var.project_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "webhook_invoke_worker" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.worker.arn]
  }
}

resource "aws_iam_role_policy" "webhook_invoke_worker" {
  name   = "${var.project_name}-invoke-worker"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.webhook_invoke_worker.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${local.worker_function_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "bot" {
  function_name = var.project_name
  role          = aws_iam_role.lambda.arn
  filename      = var.lambda_zip

  package_type  = "Zip"
  architectures = ["arm64"]
  runtime       = "provided.al2023"
  handler       = "bootstrap"

  memory_size = var.lambda_memory_size
  timeout     = 30

  source_code_hash = filebase64sha256(var.lambda_zip)

  environment {
    variables = local.webhook_environment_variables
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy.webhook_invoke_worker]
}

resource "aws_lambda_function" "worker" {
  function_name = local.worker_function_name
  role          = aws_iam_role.lambda.arn
  filename      = var.worker_lambda_zip

  package_type  = "Zip"
  architectures = ["arm64"]
  runtime       = "provided.al2023"
  handler       = "bootstrap"

  memory_size = var.lambda_memory_size
  timeout     = 900

  ephemeral_storage {
    size = 10240
  }

  source_code_hash = filebase64sha256(var.worker_lambda_zip)

  environment {
    variables = local.worker_environment_variables
  }

  depends_on = [aws_cloudwatch_log_group.worker]
}

resource "aws_lambda_function_event_invoke_config" "worker" {
  function_name                = aws_lambda_function.worker.function_name
  maximum_event_age_in_seconds = 900
  maximum_retry_attempts       = 0
}

resource "aws_lambda_function_url" "bot" {
  function_name      = aws_lambda_function.bot.function_name
  authorization_type = "NONE"
}

resource "aws_lambda_permission" "function_url" {
  statement_id           = "AllowFunctionUrlInvoke"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.bot.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "function_url_invoke" {
  statement_id             = "AllowFunctionUrlInvokeFunction"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.bot.function_name
  principal                = "*"
  invoked_via_function_url = true
}
