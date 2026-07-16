variable "aws_region" {
  type    = string
  default = "us-west-2"
}

variable "my_ip_cidr" {
  description = "Your public IP as a /32 CIDR (curl ifconfig.me). The security group opens SSH/8080/9092/3000 to this and nothing else."
  type        = string
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "lakehouse_bucket_name" {
  description = "S3 bucket names are globally unique across all of AWS -- \"san-pedro-bay-lakehouse\" is very likely already taken by someone else's account. Override this."
  type        = string
  default     = "san-pedro-bay-lakehouse"
}

variable "tfstate_bucket_name" {
  type    = string
  default = "spb-pipeline-tfstate"
}

variable "lakehouse_bronze_retention_days" {
  type    = number
  default = 14
}

variable "instance_type" {
  description = "t3.micro: AWS's actual standard Free Tier compute (750 hrs/month, 12 months from account creation). Note this is x86_64, not arm64 -- Graviton (t4g) instances aren't part of the classic Free Tier at any size, t4g.micro included, so arm64 and \"entirely free tier\" are mutually exclusive on AWS EC2 today. Verify current terms in your account's Billing -> Free Tier page regardless; these change without much notice."
  type        = string
  default     = "t3.micro"
}
