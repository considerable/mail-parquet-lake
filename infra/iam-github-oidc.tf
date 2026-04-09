variable "s3_bucket" {
  default = "mail-parquet-lake"
}

variable "s3_prefix" {
  default = "your-prefix"
}

variable "github_org" {
  default = "considerable"
}

variable "github_repo" {
  default = "mail-parquet-lake"
}

# GitHub OIDC provider (create once per AWS account)
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# Role that GitHub Actions assumes
resource "aws_iam_role" "job_tracker_ci" {
  name = "job-tracker-ci"
  tags = { Project = "mail-parquet-lake" }

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_org}/${var.github_repo}:*"
          }
        }
      }
    ]
  })
}

# S3 read + write policy scoped to the gmail prefix
resource "aws_iam_role_policy" "s3_access" {
  name = "s3-access-mail-parquet-lake"
  role = aws_iam_role.job_tracker_ci.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.s3_bucket}"
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.s3_prefix}/*"]
          }
        }
      },
      {
        Sid      = "ReadObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}/*"
      },
      {
        Sid      = "WriteObjects"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}/*"
      },
      {
        Sid    = "CodeGuruReview"
        Effect = "Allow"
        Action = [
          "codeguru-reviewer:CreateCodeReview",
          "codeguru-reviewer:DescribeCodeReview",
          "codeguru-reviewer:ListRecommendations",
          "codeguru-reviewer:TagResource"
        ]
        Resource = "arn:aws:codeguru-reviewer:us-west-2:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Sid      = "BedrockInvoke"
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel"]
        # nosemgrep: terraform.lang.security.iam.no-iam-data-exfiltration.no-iam-data-exfiltration
        Resource = [
          "arn:aws:bedrock:*::foundation-model/amazon.nova-micro-v1:0",
          "arn:aws:bedrock:*:${data.aws_caller_identity.current.account_id}:inference-profile/us.amazon.nova-micro-v1:0"
        ]
      }
    ]
  })
}
