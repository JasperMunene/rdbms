#!/usr/bin/env python3
import sys
import os

# Add src to path so pesasql package can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

if __name__ == "__main__":
    try:
        from pesasql.cli import main
        main()
    except ImportError as e:
        print(f"Error starting PesaSQL: {e}")
        print("Ensure you are running from the project root.")
