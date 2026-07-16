# Canonical's official x86_64 24.04 AMI, looked up by name pattern rather
# than a hardcoded ID -- these rotate per-region as Canonical ships
# updates, same reasoning as the OCI image data source.
#
# x86_64, not arm64: t3.micro (the actual Free Tier compute type) is
# x86_64 only. Graviton (t4g, arm64) isn't part of AWS's classic Free
# Tier at any size, so "arm64" and "entirely free tier" don't both fit --
# this build picked free tier. If you want arm64 to match the OCI box
# later, that's a real (small) cost, not a config bug.
data "aws_ami" "ubuntu_amd64" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_key_pair" "pipeline" {
  key_name   = "spb-pipeline"
  public_key = file(var.ssh_public_key_path)
}

resource "aws_instance" "pipeline" {
  ami                    = data.aws_ami.ubuntu_amd64.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.pipeline.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  key_name               = aws_key_pair.pipeline.key_name

  # 30 GB is the Free Tier EBS ceiling (any combination of General
  # Purpose SSD). gp3 is the modern default and currently included, but
  # this is exactly the kind of term that's changed before -- check
  # Billing -> Free Tier if in doubt.
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name = "spb-pipeline"
  }
}
