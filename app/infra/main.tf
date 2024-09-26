provider "aws" {
  region = "ap-southeast-2"  # Change to your desired region
}

# Create the IAM Role
resource "aws_iam_role" "streamlit_glue_role" {
  name = "StreamlitGlueRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:user/${aws_iam_user.streamlit_user.name}"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach necessary policies to the role
resource "aws_iam_role_policy_attachment" "glue_policy_attachment" {
  role       = aws_iam_role.streamlit_glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# Attach necessary policies to the role
resource "aws_iam_role_policy_attachment" "athena_policy_attachment" {
  role       = aws_iam_role.streamlit_glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

# Attach necessary policies to the role
resource "aws_iam_role_policy_attachment" "s3_policy_attachment" {
  role       = aws_iam_role.streamlit_glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}


# Create an IAM user with programmatic access
resource "aws_iam_user" "streamlit_user" {
  name = "StreamlitUser"
  force_destroy = true
}

# Attach the inline policy allowing the user to assume the role
resource "aws_iam_user_policy" "assume_role_policy" {
  name = "StreamlitUserAssumeRole"
  user = aws_iam_user.streamlit_user.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = "sts:AssumeRole",
        Resource = aws_iam_role.streamlit_glue_role.arn
      }
    ]
  })
}

# Create access keys for the IAM user
resource "aws_iam_access_key" "streamlit_user_access_key" {
  user = aws_iam_user.streamlit_user.name
}

# Output the IAM user's Access Key and Secret Access Key
output "streamlit_user_access_key" {
  value = aws_iam_access_key.streamlit_user_access_key.id
  sensitive = true
}

output "streamlit_user_secret_access_key" {
  value = aws_iam_access_key.streamlit_user_access_key.secret
  sensitive = true
}

output "role_arn" {
  value = aws_iam_role.streamlit_glue_role.arn
}
