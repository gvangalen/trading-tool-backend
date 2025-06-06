# start_backend.py
import sys, os
sys.path.insert(0, os.path.abspath("backend"))  # zorgt dat utils gevonden wordt
from backend.main import app
