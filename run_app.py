#!/usr/bin/env python3
import sys
import os

# Add src to path so pesasql package can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from server.main import create_app

if __name__ == "__main__":
    app = create_app()
    print("Starting PesaSQL Web Interface on http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
