#!/usr/bin/env python3
"""
Graph AI Wallet Native Transfer Ingestion Service
Main entry point for the ingestion service that ingests native transfers.
"""

import sys
import json
import time
from pathlib import Path


def main():
    """Main function - Hello World for now."""
    print("🚀 Graph AI Wallet Native Transfer Ingestion Service")
    print("=" * 50)
    print("Hello World! Service is running...")
    print(f"Python version: {sys.version}")
    print(f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Keep the service running
    try:
        while True:
            print("💤 Service is alive...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        print("✅ Service shutdown complete")


if __name__ == "__main__":
    main()
