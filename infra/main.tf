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
  lambda_environment_variables = merge(
    {
      TELEGRAM_BOT_TOKEN             = var.telegram_bot_token
      TELEGRAM_WEBHOOK_SECRET        = var.telegram_webhook_secret
      RUTRACKER_BASE_URLS            = var.rutracker_base_urls
      SEARCH_LIMIT                   = "10"
      RUTRACKER_HTTP_TIMEOUT_SECONDS = "30"
      RUTRACKER_HTTP_MAX_ATTEMPTS    = "2"
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

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}"
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
  timeout     = 900

  ephemeral_storage {
    size = 10240
  }

  source_code_hash = filebase64sha256(var.lambda_zip)

  environment {
    variables = local.lambda_environment_variables
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
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
