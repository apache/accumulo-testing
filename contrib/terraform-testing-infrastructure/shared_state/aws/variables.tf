variable "bucket" {
  default     = ""
  type        = string
  description = "S3 bucket name for storing shared state. If not supplied, a name will be generated."
}

variable "bucket_acl" {
  default     = "private"
  type        = string
  description = "The ACL to use for the S3 bucket. Defaults to private."
  validation {
    condition     = contains(["private", "public-read", "public-read-write", "aws-exec-read", "authenticated-read", "log-delivery-write"], var.bucket_acl)
    error_message = "The value of bucket_acl must be one of private, public-read, public-read-write, aws-exec-read, authenticated-read, or log-delivery-write."
  }
}

variable "bucket_force_destroy" {
  default     = false
  type        = bool
  description = "If true, upon terraform destroy, the bucket will be deleted even if it is not empty."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region to use for S3 bucket."
}

variable "dynamodb_table_name" {
  default     = "accumulo-testing-tf-locks"
  type        = string
  description = "DynamoDB table name for storing shared state."
}
