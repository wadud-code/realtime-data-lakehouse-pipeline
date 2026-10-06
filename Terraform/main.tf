
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure AWS Provider to target LocalStack
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true
  endpoints {
    s3  = "http://localhost:4566"
    iam = "http://localhost:4566"
  }
}

# 1. Provision Bronze Raw Bucket
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "tf-data-lake-bronze"
}

# 2. Provision Silver Cleaned Bucket
resource "aws_s3_bucket" "silver_bucket" {
  bucket = "tf-data-lake-silver"
}

# 3. Create IAM Role for Airflow Execution
resource "aws_iam_role" "airflow_execution_role" {
  name = "airflow_s3_pipeline_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "airflow.amazonaws.com"
        }
      }
    ]
  })
}

# 4. Attach S3 Read/Write IAM Policy
resource "aws_iam_policy" "s3_access_policy" {
  name        = "AirflowS3AccessPolicy"
  description = "Allows Airflow to read Bronze and write to Silver"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.bronze_bucket.arn,
          "${aws_s3_bucket.bronze_bucket.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.silver_bucket.arn,
          "${aws_s3_bucket.silver_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role       = aws_iam_role.airflow_execution_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}


# 1. Create an SQS Queue for S3 File Events
resource "aws_sqs_queue" "s3_event_queue" {
  name = "s3-raw-file-ingestion-queue"
}

# 2. Define SQS Queue Policy allowing S3 to publish messages
resource "aws_sqs_queue_policy" "sqs_policy" {
  queue_url = aws_sqs_queue.s3_event_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.s3_event_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_s3_bucket.bronze_bucket.arn
          }
        }
      }
    ]
  })
}

# 3. Configure S3 Event Notification to trigger SQS on CSV upload
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.bronze_bucket.id

  queue {
    queue_arn     = aws_sqs_queue.s3_event_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".csv"
  }
}

# Output SQS Queue URL for Airflow configuration
output "sqs_queue_url" {
  value = aws_sqs_queue.s3_event_queue.id
}
