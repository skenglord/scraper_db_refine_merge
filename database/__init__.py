"""
MongoDB Event Data Quality System

This module provides tools for managing event data with quality scoring in MongoDB.
"""
import sys
import os
# Temporarily add package directory to path for relative imports
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from .mongodb_setup import MongoDBSetup
from .quality_scorer import QualityScorer
from .data_migration import DataMigration

# Clean up sys.path if added
if _current_dir in sys.path and sys.path[0] == _current_dir :
    sys.path.pop(0)


__version__ = "1.0.0"
__all__ = ["MongoDBSetup", "QualityScorer", "DataMigration"]