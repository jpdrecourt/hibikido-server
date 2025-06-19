"""
Hibikidō - Semantic Search Engine for Musical Sounds and Effects
================================================================

A semantic search engine that maps natural language descriptions to audio content
using neural embeddings and hierarchical database design.

Main Components:
- HibikidoDatabase: MongoDB interface for hierarchical audio data
- EmbeddingManager: FAISS-based semantic search
- TextProcessor: Hierarchical context processing
- HibikidoServer: OSC server for real-time interaction
"""

__version__ = "0.1.0"
__author__ = "Your Name"

# Main exports
from .database_manager import HibikidoDatabase
from .embedding_manager import EmbeddingManager
from .text_processor import TextProcessor
from .main_server import HibikidoServer

# Optional imports with graceful fallbacks
try:
    from .csv_importer import CSVImporter
except ImportError:
    pass

try:
    from .osc_handler import OSCHandler
except ImportError:
    pass

__all__ = [
    "HibikidoDatabase",
    "EmbeddingManager",
    "TextProcessor",
    "HibikidoServer",
    "CSVImporter",
    "OSCHandler",
]