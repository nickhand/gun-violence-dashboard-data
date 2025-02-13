from importlib.metadata import version
from pathlib import Path

__version__ = version(__package__)

DATA_DIR = Path(__file__).parent.absolute() / "data"
EPSG = 2272

# Where we upload data files to s3
BUCKET_NAME = "philly-gun-violence-map"
