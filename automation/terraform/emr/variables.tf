variable "name" {
  type = string
  default = "EMRSpark"
}

variable "region" {
  type = string
  default = "us-east-1"
}

variable "key_name" {
  type = string
  default = "EC2-tutorial"
}

variable "release_label" {
  type = string
  default = "emr-5.29.0"
}

variable "applications" {
  type = list(string)
  default = ["Hadoop", "Spark"]
}

variable "master_instance_type" {
  default = "m4.large"
}

variable "master_ebs_size" {
  type = number
  default = 20
}

variable "core_instance_type" {
  default = "m4.large"
}

variable "core_instance_count" {
  type = number
  default = 1
}

variable "core_ebs_size" {
  type = number
  default = 20
}

variable "emr_service_role" {
  type = string
  default = "EMR_DefaultRole"
}

variable "ec2_instance_profile" {
  type = string
  default = "EMR_EC2_DefaultRole"
}