# terraform-admin (the IAM user this Terraform runs as) is deliberately
# NOT a resource here -- it's created manually per your Stage 1 checklist,
# same chicken-and-egg reasoning as the tfstate bucket: a user managing
# its own permissions via the tool it authenticates that tool with is a
# self-lockout risk (revoke your own apply permission mid-apply and
# you're stuck). This file only manages the EC2 instance's role, a
# different actor.

data "aws_iam_policy_document" "instance_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "spb-pipeline-instance-role"
  assume_role_policy = data.aws_iam_policy_document.instance_assume_role.json
}

# Scoped to exactly the lakehouse bucket -- not s3:* account-wide, not
# the tfstate bucket (the instance has no business touching Terraform's
# own state).
data "aws_iam_policy_document" "instance_lakehouse_access" {
  statement {
    sid       = "ListLakehouseBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid = "ReadWriteLakehouseObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
}

resource "aws_iam_role_policy" "instance_lakehouse_access" {
  name   = "spb-pipeline-lakehouse-access"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.instance_lakehouse_access.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "spb-pipeline-instance-profile"
  role = aws_iam_role.instance.name
}
