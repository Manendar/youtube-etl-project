#!/bin/bash

# Create kaggle credentials file at runtime from environment variables

if [ -n "$KAGGLE_USERNAME" ] && [ -n "$KAGGLE_KEY" ]; then
    mkdir -p /root/.config/kaggle
    echo "{\"username\":\"${KAGGLE_USERNAME}\",\"key\":\"${KAGGLE_KEY}\"}" > /root/.config/kaggle/kaggle.json
    chmod 600 /root/.config/kaggle/kaggle.json
    echo "Kaggle credentials configured successfully"
else
    echo "WARNING: KAGGLE_USERNAME or KAGGLE_KEY not set"
fi

# Start SSH server
exec /usr/sbin/sshd -D
