#!/bin/bash

# Setup script for EC2 instance
# Run this once on your EC2 instance to prepare it for deployment

echo "🚀 Setting up EC2 instance for Ingestion Service deployment..."

# Create application directory in home directory
mkdir -p ~/ingestion-service
cd ~/ingestion-service

# Clone the repository (replace with your actual repo URL)
echo "📥 Cloning repository..."
git clone https://github.com/YOUR_USERNAME/graph-ai-native-transfers-ingestion-service.git .

# Copy docker-compose.yml (it's already in the repo)
# The file will be copied when you deploy via GitHub Actions
echo "📝 docker-compose.yml will be managed by the repository"

echo "✅ EC2 setup complete!"
echo "📋 Next steps:"
echo "1. Set up GitHub Secrets in your repository:"
echo "   - AWS_EC2_HOST: Your EC2 instance IP/hostname"
echo "   - AWS_EC2_USERNAME: Your EC2 username (usually 'ubuntu' or 'ec2-user')"
echo "   - AWS_EC2_SSH_KEY: Your private SSH key (paste entire key including BEGIN/END lines)"
echo "   - VM_APP_PATH: ~/ingestion-service"
echo "   - PAT_TOKEN: Your GitHub Personal Access Token"
echo "2. Update the git clone URL in this script (line 14) with your actual repository URL"
echo "3. Push changes to trigger deployment"
echo "4. Monitor deployment in GitHub Actions"
