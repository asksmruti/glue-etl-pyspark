provider "aws" {
  version = "~> 2.0"
  region  = "eu-central-1"
}

## The glue role name which lives in data-owners account
variable GLUE_ROLE{
  type = string
  default = "role-name"
}

## ETL script path in S3
variable SCRIPT_PATH{
  type = string
  default = "s3://<bucket>/aws_glue_pyspark_etl.py"
}

## S3 Bucket Name where data resides
variable BUCKET_NAME{
  type = string
  default = "<bucket_name>"
}

## S3 Path where to write data
variable S3_WRITE_PATH{
  type = string
  default = "<bucket_name>/<some_prefix>/"
}

## S3 Input path where raw data lives, one level up to partition-date=yyyy-dd-mm
variable INPUT_PATH{
  type = string
  default = "<bucket_name>/<some_other_path>/"
}

## Assume role ARN which lives in Datalake account
variable ASSUME_ROLE_ARN{
  type = string
  default = "arn:aws:iam::<account_id>:role/<role-name>"
}

## Raw file format eg. json, csv etc..
variable READ_FILE_FORMAT{
  type = string
  default = "json"
}

## Write file format eg. parquet
variable WRITE_FILE_FORMAT{
  type = string
  default = "parquet"
}

## Process N days old files (excluding current date) - Minimum value should be 1
variable LOOK_BACK_DAYS{
  type = number
  default = 0
}

## Partitioning field
variable PARTITION_COL{
  type = string
  default = "<partition_col>"
}

## To flatten ALL fields <!-- Its risky to enable --!>
variable FLATTEN_COL{
  type = string
  default = "false"
}

## Set the script language
variable LANG{
  type = string
  default = "python"
}

## Set the environment
variable ENV{
  type = string
  default = "prod"
}

data "aws_iam_role" "GLUE_ROLE" {
  name = var.GLUE_ROLE
}

resource "aws_glue_job" "glue-etl" {
  glue_version = "1.0"
  name         = "glue-etl-test-smruti"
  description  = "This is a test job"
  role_arn     = data.aws_iam_role.GLUE_ROLE.arn
  max_capacity = 2.0
  max_retries  = 1
  timeout      = 30

  command {
    name            = "glueetl"
    script_location = var.SCRIPT_PATH
    python_version  = "3"
  }

  default_arguments = {    
    "--enable-glue-datacatalog" = "true"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--job-language"        = var.LANG
    "--ENV"                 = var.ENV
    "--ROLE_ARN"            = data.aws_iam_role.GLUE_ROLE.arn
    "--ASSUME_ROLE_ARN"     = var.ASSUME_ROLE_ARN
    "--BUCKET_NAME"         = var.BUCKET_NAME
    "--S3_WRITE_PATH"       = var.S3_WRITE_PATH
    "--INPUT_PATH"          = var.INPUT_PATH
    "--READ_FILE_FORMAT"    = var.READ_FILE_FORMAT
    "--WRITE_FILE_FORMAT"   = var.WRITE_FILE_FORMAT
    "--LOOK_BACK_DAYS"      = var.LOOK_BACK_DAYS
    "--PARTITION_COL"       = var.PARTITION_COL
    "--FLATTEN_COL"         = var.FLATTEN_COL
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

resource "aws_glue_trigger" "glue-etl-trigger" {
  name     = "glue-etl-trigger"
  #schedule = "cron(15 12 * * ? *)"
  #type     = "SCHEDULED"
  type     = "ON_DEMAND"

  actions {
    job_name = "${aws_glue_job.glue-etl.name}"
  }
}
