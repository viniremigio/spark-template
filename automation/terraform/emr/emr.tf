provider "aws" {
  region     = "us-east-1"
}

resource "aws_default_subnet" "default_az1" {
  availability_zone = "us-east-1a"
}

resource "aws_emr_cluster" "emr-spark-cluster" {
  name                              = var.name
  release_label                     = var.release_label
  applications                      = var.applications
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    subnet_id                         = aws_default_subnet.default_az1.id
    key_name                          = var.key_name
    instance_profile                  = var.ec2_instance_profile
  }

  ebs_root_volume_size = "12"

  master_instance_group {
    name           = "EMR master"
    instance_type  = var.master_instance_type
    instance_count = 1

    ebs_config {
      size                 = var.master_ebs_size
      type                 = "gp2"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    name           = "EMR slave"
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count

    ebs_config {
      size                 = var.core_ebs_size
      type                 = "gp2"
      volumes_per_instance = 1
    }
  }

  service_role     = var.emr_service_role

  configurations_json = <<EOF
    [
    {
    "Classification": "spark-defaults",
      "Properties": {
      "maximizeResourceAllocation": "true",
      "spark.dynamicAllocation.enabled": "true"
      }
    }
  ]
  EOF
}