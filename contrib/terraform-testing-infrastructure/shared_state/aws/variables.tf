#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
