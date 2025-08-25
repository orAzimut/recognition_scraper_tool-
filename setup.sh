

# Set ownership to match the container user (UID 1000)
# This ensures the container can write to it
sudo chown -R 1000:1000 output_json

# Set appropriate permissions
chmod -R 755 output_json

echo "✅ Directory setup complete!"
echo "📁 output_json directory is ready for Docker container"