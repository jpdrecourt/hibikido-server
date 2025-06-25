"""
Hibikidō - Semantic Search Engine for Musical Sounds and Effects
================================================================

A semantic search engine that maps natural language descriptions to audio content
using neural embeddings and hierarchical database design, with real-time orchestration
for time-frequency niche management.

Main Components:
- HibikidoDatabase: MongoDB interface for hierarchical audio data
- EmbeddingManager: FAISS-based semantic search
- TextProcessor: Hierarchical context processing
- HibikidoServer: OSC server for real-time interaction
- Orchestrator: Real-time time-frequency niche management
"""

__version__ = "0.1.0"
__author__ = "Jean-Philippe Drecourt"

# Main exports
from .database_manager import HibikidoDatabase
from .embedding_manager import EmbeddingManager
from .text_processor import TextProcessor
from .main_server import HibikidoServer
from .orchestrator import Orchestrator
from .osc_handler import OSCHandler

__all__ = [
    "HibikidoDatabase",
    "EmbeddingManager",
    "TextProcessor", 
    "HibikidoServer",
    "Orchestrator",
    "CSVImporter",
    "OSCHandler",
]