data "aws_region" "current" {}

locals {
  otel_log_group_arn = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/mail-parquet-lake/otel"
}

resource "aws_kms_key" "lake_otel" {
  description             = "CloudWatch encryption for mail-parquet-lake telemetry"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AccountKeyAdministration"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "CloudWatchLogsForThisGroup"
        Effect    = "Allow"
        Principal = { Service = "logs.${data.aws_region.current.name}.amazonaws.com" }
        Action    = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
        Resource  = "*"
        Condition = {
          ArnEquals = { "kms:EncryptionContext:aws:logs:arn" = local.otel_log_group_arn }
        }
      }
    ]
  })
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "lake_otel" {
  name          = "alias/mail-parquet-lake-otel"
  target_key_id = aws_kms_key.lake_otel.key_id
}

resource "aws_cloudwatch_log_group" "lake_otel" {
  name              = "/mail-parquet-lake/otel"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.lake_otel.arn
}

resource "aws_iam_role_policy" "lake_otel" {
  name = "mail-lake-opentelemetry"
  role = aws_iam_role.job_tracker_ci.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:DescribeLogStreams", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.lake_otel.arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
        Resource = aws_kms_key.lake_otel.arn
        Condition = {
          StringEquals = { "kms:ViaService" = "logs.${data.aws_region.current.name}.amazonaws.com" }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments"]
        Resource = "*"
      }
    ]
  })
}
