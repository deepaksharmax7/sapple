# tests/test_handler.py

import sys
import os

# Add the root directory (sapple) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.handler import VimLoad

vim_load = VimLoad()

def test_some_mthd():
    pass

