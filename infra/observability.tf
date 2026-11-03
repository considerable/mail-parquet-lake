resource "aws_cloudwatch_log_group" "lake_otel" {
  name              = "/mail-parquet-lake/otel"
  retention_in_days = 30
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
        Action   = ["xray:PutTraceSegments"]
        Resource = "*"
      }
    ]
  })
}
