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

variable "location" {
  default     = "eastus"
  type        = string
  description = "The region where resources will be deployed."
}

variable "resource_group_name" {
  default     = "accumulo-testing-tf-state"
  type        = string
  description = "Name of the resource group that holds the shared state storage account."
}

variable "storage_account_name" {
  default     = "accumulotesttfsteast"
  type        = string
  description = "Name of the storage account that will hold shared state."
  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "The storage_account_name variable name must be letters and numbers and be 3-24 characters in length."
  }
}

variable "storage_container_name" {
  default = "accumulo-testing-tf-state"
  type    = string
  validation {
    condition     = can(regex("^[-a-z0-9]{3,63}$", var.storage_container_name))
    error_message = "The storage_container_name variable name must be letters and numbers and be 3-63 characters in length."
  }
}
