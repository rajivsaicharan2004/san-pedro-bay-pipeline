output "instance_public_ip" {
  description = "SSH here: ssh ubuntu@<this>"
  value       = aws_instance.pipeline.public_ip
}

output "lakehouse_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}
