#!/bin/bash

# Setup script for EC2 instance
# Run this once on your EC2 instance to prepare it for deployment

echo "🚀 Setting up EC2 instance for Ingestion Service deployment..."

# Create application directory
mkdir -p ~/ingestion-service
cd ~/ingestion-service

# Clone the repository (replace with your actual repo URL)
echo "📥 Cloning repository..."
git clone https://github.com/YOUR_USERNAME/graph-ai-wallet-native-transfers-fetcher.git .

# Create docker-compose.yml
echo "📝 Creating docker-compose.yml..."
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  ingestion-service:
    build: .
    container_name: ingestion-service
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    environment:
      - PYTHONUNBUFFERED=1
EOF

echo "✅ EC2 setup complete!"
echo "📋 Next steps:"
echo "1. Set up GitHub Secrets in your repository:"
echo "   - EC2_HOST: Your EC2 instance IP/hostname"
echo "   - EC2_USERNAME: Your EC2 username (usually 'ubuntu' or 'ec2-user')"
echo "   - EC2_SSH_KEY: Your private SSH key"
echo "2. Update the git clone URL in this script with your actual repository URL"
echo "3. Push changes to trigger deployment"
echo "4. Monitor deployment in GitHub Actions"
