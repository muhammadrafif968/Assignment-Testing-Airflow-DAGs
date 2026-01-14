import sys
import os

DAGS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if DAGS_PATH not in sys.path:
    sys.path.insert(0, DAGS_PATH)