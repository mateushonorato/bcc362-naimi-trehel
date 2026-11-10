#!/usr/bin/env python3
"""Convenience runner for results/generate_charts.py."""
import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    script_path = Path(__file__).resolve().parent / "results" / "generate_charts.py"
    runpy.run_path(str(script_path), run_name="__main__")
