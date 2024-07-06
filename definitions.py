# definitions.py
from pathlib import Path

# Define the base directory based on the location of this script
BASE_DIR = Path(__file__).resolve().parent

# Paths to various data directories
DATA_DIR = BASE_DIR / 'data' 
OUTPUT_DIR = BASE_DIR / 'output'

