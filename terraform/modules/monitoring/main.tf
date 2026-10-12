locals {
  dashboard_name = var.dashboard_name != "" ? var.dashboard_name : "${var.project_name}-data-platform-dashboard-${var.environment}"
}

# ==============================================================================
# CloudWatch Metric Alarm: Manifest Validator Lambda Errors
# ==============================================================================
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_name}-manifest-validator-errors-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when Manifest Validator Lambda encounters any invocation execution error"
  alarm_actions       = [var.sns_alerts_topic_arn]

  dimensions = {
    FunctionName = var.lambda_function_name
  }

  tags = merge(
    var.tags,
    {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Service     = "Lambda"
    }
  )
}

# ==============================================================================
# Production-Grade CloudWatch Unified Platform Dashboard
# ==============================================================================
resource "aws_cloudwatch_dashboard" "platform_dashboard" {
  dashboard_name = local.dashboard_name

  dashboard_body = jsonencode({
    widgets = [
      # ----------------------------------------------------------------------
      # Widget 1: Platform Header & Architecture Banner
      # ----------------------------------------------------------------------
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 2
        properties = {
          markdown = "## 🚀 GitOps Modern Data Platform — Operational Health & Observability Dashboard (`${var.environment}`)\n**Data Pipeline Architecture**: Multi-Source S3 Ingestion (API, File Share, Oracle) ➔ Lambda Manifest Barrier ➔ DynamoDB State Coordination ➔ SQS Ready Queue / SNS ➔ Airflow Orchestration ➔ Parallel Glue PySpark ETL (Parquet / Delta Lake / Iceberg) ➔ Amazon Athena & Databricks Unity Catalog."
        }
      },

      # ----------------------------------------------------------------------
      # Widget 2: Alarms Status Widget
      # ----------------------------------------------------------------------
      {
        type   = "alarm"
        x      = 0
        y      = 2
        width  = 6
        height = 4
        properties = {
          title = "🚨 Platform Alarms Status"
          alarms = [
            var.sqs_dlq_alarm_arn,
            aws_cloudwatch_metric_alarm.lambda_errors.arn
          ]
        }
      },

      # ----------------------------------------------------------------------
      # Widget 3: KPI - SQS DLQ Messages (Red if > 0)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 6
        y      = 2
        width  = 4
        height = 4
        properties = {
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", var.sqs_dlq_name, { "stat" : "Maximum", "color" : "#d62728", "label" : "DLQ Messages" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          period = 60
          title  = "Dead-Letter Queue (DLQ)"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 4: KPI - SQS Batch Ready Queue Messages
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 10
        y      = 2
        width  = 4
        height = 4
        properties = {
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", var.sqs_primary_queue_name, { "stat" : "Maximum", "color" : "#1f77b4", "label" : "Batch Messages Waiting" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          period = 60
          title  = "Primary SQS Ready Depth"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 5: KPI - Lambda Invocations & Errors
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 14
        y      = 2
        width  = 5
        height = 4
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", var.lambda_function_name, { "stat" : "Sum", "color" : "#2ca02c", "label" : "Invocations" }],
            [".", "Errors", ".", ".", { "stat" : "Sum", "color" : "#d62728", "label" : "Errors" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          period = 300
          title  = "Lambda Barrier Runs / Errors"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 6: KPI - DynamoDB Throttles & Errors
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 19
        y      = 2
        width  = 5
        height = 4
        properties = {
          metrics = [
            ["AWS/DynamoDB", "SystemErrors", "TableName", var.dynamodb_table_name, { "stat" : "Sum", "color" : "#d62728", "label" : "System Errors" }],
            [".", "UserErrors", ".", ".", { "stat" : "Sum", "color" : "#ff7f0e", "label" : "User Errors" }]
          ]
          view   = "singleValue"
          region = var.aws_region
          period = 300
          title  = "DynamoDB Barrier Errors"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 7: Log Insights Query — Recent Lambda Barrier Failures
      # ----------------------------------------------------------------------
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          query   = "SOURCE '/aws/lambda/${var.lambda_function_name}' | fields @timestamp, @message, @logStream | filter @message like /(?i)error|exception|fail|traceback/ | sort @timestamp desc | limit 20"
          region  = var.aws_region
          stacked = false
          title   = "🔍 Root-Cause Analysis: Recent Lambda Barrier Failures & Exceptions"
          view    = "table"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 8: Log Insights Query — Recent Glue ETL Execution Errors
      # ----------------------------------------------------------------------
      {
        type   = "log"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          query   = "SOURCE '/aws-glue/jobs/error' | fields @timestamp, @message, @logStream | filter @message like /(?i)error|exception|fail/ | sort @timestamp desc | limit 20"
          region  = var.aws_region
          stacked = false
          title   = "🔍 Root-Cause Analysis: Recent AWS Glue PySpark ETL Errors"
          view    = "table"
        }
      },

      # ----------------------------------------------------------------------
      # Widget 9: Lambda Invocations, Errors & Throttles
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", var.lambda_function_name, { "stat" : "Sum", "color" : "#2ca02c", "label" : "Invocations" }],
            [".", "Errors", ".", ".", { "stat" : "Sum", "color" : "#d62728", "yAxis" : "right", "label" : "Errors (Right Axis)" }],
            [".", "Throttles", ".", ".", { "stat" : "Sum", "color" : "#ff7f0e", "yAxis" : "right", "label" : "Throttles (Right Axis)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "Lambda Manifest Validator — Invocations, Errors & Throttles"
          yAxis = {
            left  = { min = 0, label = "Count" },
            right = { min = 0, label = "Errors / Throttles" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 10: Lambda Latency Duration (Avg, p90, Max)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 12
        y      = 12
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", var.lambda_function_name, { "stat" : "Average", "color" : "#1f77b4", "label" : "Avg Duration (ms)" }],
            ["...", { "stat" : "p90", "color" : "#ff7f0e", "label" : "p90 Duration (ms)" }],
            ["...", { "stat" : "Maximum", "color" : "#d62728", "label" : "Max Duration (ms)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "Lambda Execution Latency (Avg, p90, Max ms)"
          yAxis = {
            left = { min = 0, label = "Duration (ms)" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 11: DynamoDB State Barrier Capacity & Latency
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", var.dynamodb_table_name, { "stat" : "Sum", "color" : "#1f77b4", "label" : "Consumed Read Units" }],
            [".", "ConsumedWriteCapacityUnits", ".", ".", { "stat" : "Sum", "color" : "#2ca02c", "label" : "Consumed Write Units" }],
            [".", "SuccessfulRequestLatency", ".", ".", "Operation", "PutItem", { "stat" : "Average", "color" : "#9467bd", "yAxis" : "right", "label" : "PutItem Latency (ms)" }],
            [".", "SuccessfulRequestLatency", ".", ".", "Operation", "GetItem", { "stat" : "Average", "color" : "#8c564b", "yAxis" : "right", "label" : "GetItem Latency (ms)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "DynamoDB Barrier State — Capacity Consumption & Latency"
          yAxis = {
            left  = { min = 0, label = "Capacity Units" },
            right = { min = 0, label = "Latency (ms)" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 12: SQS Queue Depths (Primary vs DLQ)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", var.sqs_primary_queue_name, { "stat" : "Maximum", "color" : "#1f77b4", "label" : "Primary Queue (Batch Ready)" }],
            [".", "ApproximateNumberOfMessagesNotVisible", ".", ".", { "stat" : "Maximum", "color" : "#aec7e8", "label" : "Primary In-Flight" }],
            [".", "ApproximateNumberOfMessagesVisible", "QueueName", var.sqs_dlq_name, { "stat" : "Maximum", "color" : "#d62728", "label" : "DLQ Messages (Failure Alert)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "SQS Queue Depths: Ready Messages vs In-Flight vs DLQ"
          annotations = {
            horizontal = [
              { "color" : "#d62728", "label" : "DLQ Alert Threshold (>0)", "value" : 1 }
            ]
          }
          yAxis = {
            left = { min = 0, label = "Message Count" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 13: SQS Message Traffic (Sent / Received / Deleted)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 24
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/SQS", "NumberOfMessagesSent", "QueueName", var.sqs_primary_queue_name, { "stat" : "Sum", "color" : "#2ca02c", "label" : "Messages Sent" }],
            [".", "NumberOfMessagesReceived", ".", ".", { "stat" : "Sum", "color" : "#1f77b4", "label" : "Messages Received (Airflow Sensor)" }],
            [".", "NumberOfMessagesDeleted", ".", ".", { "stat" : "Sum", "color" : "#9467bd", "label" : "Messages Deleted (Processed)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "SQS Traffic Flow — Sent, Received & Deleted Lifecycle"
          yAxis = {
            left = { min = 0, label = "Messages / Min" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 14: SNS Broadcasts (Batch Events & Incident Alerts)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 12
        y      = 24
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/SNS", "NumberOfMessagesPublished", "TopicName", var.sns_batch_topic_name, { "stat" : "Sum", "color" : "#1f77b4", "label" : "Batch Events Published" }],
            [".", "NumberOfNotificationsDelivered", ".", ".", { "stat" : "Sum", "color" : "#2ca02c", "label" : "Batch Notifications Delivered" }],
            [".", "NumberOfNotificationsFailed", ".", ".", { "stat" : "Sum", "color" : "#d62728", "label" : "Batch Delivery Failures" }],
            [".", "NumberOfMessagesPublished", "TopicName", var.sns_alerts_topic_name, { "stat" : "Sum", "color" : "#ff7f0e", "label" : "Incident Alerts Published" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 60
          title   = "Amazon SNS — Broadcast Volume & Notification Delivery"
          yAxis = {
            left = { min = 0, label = "Messages" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 15: AWS Glue PySpark Job Task Health (Parquet, Delta, Iceberg)
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 30
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", var.glue_parquet_job_name, "JobRunId", "ALL", { "stat" : "Sum", "color" : "#2ca02c", "label" : "Parquet Completed Tasks" }],
            [".", "glue.driver.aggregate.numFailedTasks", ".", ".", ".", ".", { "stat" : "Sum", "color" : "#d62728", "label" : "Parquet Failed Tasks" }],
            [".", "glue.driver.aggregate.numCompletedTasks", "JobName", var.glue_delta_job_name, "JobRunId", "ALL", { "stat" : "Sum", "color" : "#1f77b4", "label" : "Delta Lake Completed Tasks" }],
            [".", "glue.driver.aggregate.numFailedTasks", ".", ".", ".", ".", { "stat" : "Sum", "color" : "#e377c2", "label" : "Delta Lake Failed Tasks" }],
            [".", "glue.driver.aggregate.numCompletedTasks", "JobName", var.glue_iceberg_job_name, "JobRunId", "ALL", { "stat" : "Sum", "color" : "#0d9488", "label" : "Iceberg Completed Tasks" }],
            [".", "glue.driver.aggregate.numFailedTasks", ".", ".", ".", ".", { "stat" : "Sum", "color" : "#bcbd22", "label" : "Iceberg Failed Tasks" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 300
          title   = "AWS Glue PySpark Distributed Tasks: Completed vs Failed"
          yAxis = {
            left = { min = 0, label = "Tasks" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 16: AWS Glue Elapsed Execution Duration
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 12
        y      = 30
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Glue", "glue.driver.aggregate.elapsedTime", "JobName", var.glue_parquet_job_name, "JobRunId", "ALL", { "stat" : "Average", "color" : "#2ca02c", "label" : "Parquet Job Elapsed Seconds" }],
            [".", "glue.driver.aggregate.elapsedTime", "JobName", var.glue_delta_job_name, "JobRunId", "ALL", { "stat" : "Average", "color" : "#1f77b4", "label" : "Delta Job Elapsed Seconds" }],
            [".", "glue.driver.aggregate.elapsedTime", "JobName", var.glue_iceberg_job_name, "JobRunId", "ALL", { "stat" : "Average", "color" : "#0d9488", "label" : "Iceberg Job Elapsed Seconds" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 300
          title   = "AWS Glue ETL Execution Duration (Elapsed Seconds)"
          yAxis = {
            left = { min = 0, label = "Seconds" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 17: Amazon Athena Query Analytics
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 36
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Athena", "QueryExecutionTime", "WorkGroup", var.athena_workgroup_name, { "stat" : "Average", "color" : "#1f77b4", "label" : "Query Execution Time (ms)" }],
            [".", "EngineExecutionTime", ".", ".", { "stat" : "Average", "color" : "#ff7f0e", "label" : "Engine Execution Time (ms)" }],
            [".", "ProcessedBytes", ".", ".", { "stat" : "Sum", "color" : "#2ca02c", "yAxis" : "right", "label" : "Processed Bytes (Right Axis)" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 300
          title   = "Amazon Athena (${var.athena_workgroup_name}) — Latency & Processed Bytes"
          yAxis = {
            left  = { min = 0, label = "Time (ms)" },
            right = { min = 0, label = "Bytes" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 18: S3 Lakehouse Storage Utilization
      # ----------------------------------------------------------------------
      {
        type   = "metric"
        x      = 12
        y      = 36
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", var.bronze_bucket_name, "StorageType", "StandardStorage", { "stat" : "Average", "color" : "#8c564b", "label" : "Bronze Bucket Bytes" }],
            [".", "BucketSizeBytes", "BucketName", var.silver_bucket_name, "StorageType", "StandardStorage", { "stat" : "Average", "color" : "#7f7f7f", "label" : "Silver Bucket Bytes" }],
            [".", "NumberOfObjects", "BucketName", var.bronze_bucket_name, "StorageType", "AllStorageTypes", { "stat" : "Average", "color" : "#bcbd22", "yAxis" : "right", "label" : "Bronze Object Count" }],
            [".", "NumberOfObjects", "BucketName", var.silver_bucket_name, "StorageType", "AllStorageTypes", { "stat" : "Average", "color" : "#17becf", "yAxis" : "right", "label" : "Silver Object Count" }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = 86400
          title   = "S3 Storage Metrics — Bucket Size & Object Counts (Bronze / Silver)"
          yAxis = {
            left  = { min = 0, label = "Bytes" },
            right = { min = 0, label = "Objects" }
          }
        }
      },

      # ----------------------------------------------------------------------
      # Widget 19: Operational Runbook & Troubleshooting Playbook
      # ----------------------------------------------------------------------
      {
        type   = "text"
        x      = 0
        y      = 42
        width  = 24
        height = 4
        properties = {
          markdown = "### 🛠️ Incident Response & Operational Runbook\n- **Dead-Letter Queue (DLQ) Alert**: Inspect messages: `aws sqs receive-message --queue-url https://sqs.${var.aws_region}.amazonaws.com/790347819012/${var.sqs_dlq_name} --attribute-names All`\n- **Lambda Execution Logs**: Follow live logs: `aws logs tail /aws/lambda/${var.lambda_function_name} --follow --format short`\n- **DynamoDB State Barrier Check**: Verify batch sources: `aws dynamodb get-item --table-name ${var.dynamodb_table_name} --key '{\"batch_date\":{\"S\":\"YYYY-MM-DD\"}}'`\n- **Glue ETL Logs**: Tail errors: `aws logs tail /aws-glue/jobs/error --follow`\n- **Architecture Documentation**: Check interactive local dashboard: `open docs/index.html`."
        }
      }
    ]
  })
}
