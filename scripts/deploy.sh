#! /bin/bash
echo "Running deploy script...."
# Relies on HEX_API_KEY environment varibale.
mix hex.publish --yes
