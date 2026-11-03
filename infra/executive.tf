resource "aws_iam_role_policy" "executive" {
  name   = "mail-lake-executive"
  role   = aws_iam_role.job_tracker_ci.id
  policy = replace(file("${path.module}/../observability/executive-policy.json"), "ACCOUNT_ID", data.aws_caller_identity.current.account_id)
}

locals {
  executive_alarms = {
    ObserverMissing   = { metric = "ObserverHeartbeat", comparison = "LessThanThreshold", periods = 2 }
    SyncOverdue       = { metric = "SyncOverdue", comparison = "GreaterThanThreshold", periods = 1 }
    SourceUnavailable = { metric = "SourceErrors", comparison = "GreaterThanThreshold", periods = 1 }
    SyncDisabled      = { metric = "SyncWorkflowActive", comparison = "LessThanThreshold", periods = 1 }
  }
}

resource "aws_cloudwatch_metric_alarm" "executive" {
  for_each            = local.executive_alarms
  alarm_name          = "MailLake-Executive-${each.key}"
  namespace           = "MailParquetLake/Executive"
  metric_name         = each.value.metric
  comparison_operator = each.value.comparison
  threshold           = 0.5
  statistic           = "Maximum"
  period              = 900
  evaluation_periods  = each.value.periods
  datapoints_to_alarm = each.value.periods
  treat_missing_data  = "breaching"
  actions_enabled     = false
  alarm_actions       = []
  ok_actions          = []
}
