# start_backend.py
import sys, os
sys.path.insert(0, os.path.abspath("."))  # Voegt ./backend toe aan sys.path

from main import app
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5002)
