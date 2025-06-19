#!/usr/bin/env python3
"""
Hibikidō Server - Main Application
==================================

Music server that maps text descriptions to sounds and effects using semantic search.
Supports hierarchical database with recordings, segments, effects, and performances.

Usage:
    python main_server.py [--config config.json]

Dependencies:
    pip install sentence-transformers python-osc faiss-cpu torch pymongo pandas
"""

import signal
import sys
import json
import argparse
import logging
from typing import Dict, Any, List
from datetime import datetime

from .database_manager import HibikidoDatabase
from .embedding_manager import EmbeddingManager
from .text_processor import TextProcessor
from .csv_importer import CSVImporter
from .osc_handler import OSCHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HibikidoServer:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or self._default_config()
        
        # Initialize components
        self.db_manager = HibikidoDatabase(
            uri=self.config['mongodb']['uri'],
            db_name=self.config['mongodb']['database']
        )
        
        self.embedding_manager = EmbeddingManager(
            model_name=self.config['embedding']['model_name'],
            index_file=self.config['embedding']['index_file']
        )
        
        self.text_processor = TextProcessor()
        self.csv_importer = CSVImporter(self.text_processor)
        
        self.osc_handler = OSCHandler(
            listen_ip=self.config['osc']['listen_ip'],
            listen_port=self.config['osc']['listen_port'],
            send_ip=self.config['osc']['send_ip'],
            send_port=self.config['osc']['send_port']
        )
        
        self.is_running = False
    
    def _default_config(self) -> Dict[str, Any]:
        """Default configuration settings."""
        return {
            'mongodb': {
                'uri': 'mongodb://localhost:27017',
                'database': 'hibikido'
            },
            'embedding': {
                'model_name': 'all-MiniLM-L6-v2',
                'index_file': 'hibikido.index'
            },
            'osc': {
                'listen_ip': '127.0.0.1',
                'listen_port': 9000,
                'send_ip': '127.0.0.1',
                'send_port': 9001
            },
            'search': {
                'top_k': 10,
                'min_score': 0.3
            }
        }
    
    def initialize(self) -> bool:
        """Initialize all components."""
        logger.info("Initializing Hibikidō Server...")
        
        # Initialize database
        if not self.db_manager.connect():
            logger.error("Failed to connect to database")
            return False
        
        # Initialize embedding system
        if not self.embedding_manager.initialize():
            logger.error("Failed to initialize embedding system")
            return False
        
        # Initialize OSC
        if not self.osc_handler.initialize():
            logger.error("Failed to initialize OSC")
            return False
        
        # Register OSC handlers
        self._register_osc_handlers()
        
        logger.info("All components initialized successfully")
        return True
    
    def _register_osc_handlers(self):
        """Register OSC message handlers."""
        handlers = {
            'search': self._handle_search,
            'add_segment': self._handle_add_segment,
            'add_preset': self._handle_add_preset,
            'import_csv': self._handle_import_csv,
            'rebuild_index': self._handle_rebuild_index,
            'stats': self._handle_stats,
            'stop': self._handle_stop
        }
        
        self.osc_handler.register_handlers(handlers)
    
    def start(self):
        """Start the server."""
        try:
            logger.info("Starting Hibikidō Server...")
            
            # Setup graceful shutdown
            signal.signal(signal.SIGINT, self._shutdown_handler)
            signal.signal(signal.SIGTERM, self._shutdown_handler)
            
            # Start OSC server
            server = self.osc_handler.start_server()
            if not server:
                logger.error("Failed to start OSC server")
                return
            
            self.is_running = True
            
            # Send ready signal
            self.osc_handler.send_ready()
            
            # Print startup banner
            self._print_banner()
            
            # Start serving
            logger.info("Server ready - waiting for OSC messages...")
            server.serve_forever()
            
        except Exception as e:
            logger.error(f"Server error: {e}")
            self.shutdown()
    
    def _print_banner(self):
        """Print startup banner with information."""
        config = self.config
        stats = self.db_manager.get_stats()
        
        print("\n" + "="*70)
        print("🎵 HIBIKIDŌ SERVER READY 🎵")
        print("="*70)
        print(f"Database: {stats.get('segments', 0)} segments, "
              f"{stats.get('presets', 0)} presets, "
              f"{stats.get('total_searchable_items', 0)} searchable")
        print(f"FAISS Index: {self.embedding_manager.get_total_embeddings()} embeddings")
        print(f"Listening: {config['osc']['listen_ip']}:{config['osc']['listen_port']}")
        print(f"Sending: {config['osc']['send_ip']}:{config['osc']['send_port']}")
        print("\nOSC Commands:")
        print("  /search \"query text\"           - semantic search across segments and presets")
        print("  /add_segment \"text\" metadata   - add new segment")
        print("  /add_preset \"text\" metadata    - add new effect preset")
        print("  /import_csv \"filepath\"         - bulk import from CSV")
        print("  /rebuild_index                  - rebuild FAISS index from database")
        print("  /stats                          - database statistics")
        print("  /stop                           - shutdown server")
        print("="*70)
        print()
    
    # OSC Message Handlers
    
    def _handle_search(self, unused_addr: str, *args):
        """Handle search requests - returns full MongoDB documents."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            query = parsed.get('arg1', '').strip()
            
            if not query:
                self.osc_handler.send_error("search requires query text")
                return
            
            logger.info(f"Search query: '{query}'")
            
            # Search with MongoDB lookups
            results = self.embedding_manager.search(
                query, 
                self.config['search']['top_k'],
                db_manager=self.db_manager
            )
            
            if not results:
                self.osc_handler.send_confirm("no matches found")
                return
            
            # Send full documents via OSC
            self._send_search_results(results)
            logger.info(f"Search '{query}' returned {len(results)} results")
            
        except Exception as e:
            error_msg = f"search failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _send_search_results(self, results: List[Dict[str, Any]]):
        """Send search results as full documents via OSC."""
        try:
            # Send each document as separate OSC message
            for i, result in enumerate(results):
                collection = result["collection"]
                document = result["document"]
                score = result["score"]
                
                # Send the full document
                self.osc_handler.client.send_message("/result", [
                    i,                           # Result index
                    collection,                  # Collection name
                    score,                       # Similarity score
                    str(document)                # Full MongoDB document as string
                ])
            
            # Send completion message
            self.osc_handler.client.send_message("/search_complete", len(results))
            
        except Exception as e:
            logger.error(f"Failed to send search results: {e}")
            self.osc_handler.send_error(f"failed to send results: {e}")
    
    def _handle_add_segment(self, unused_addr: str, *args):
        """Handle add segment requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            embedding_text = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not embedding_text:
                self.osc_handler.send_error("add_segment requires embedding text")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            # Required fields with defaults
            segment_id = metadata.get('segment_id', f"seg_{datetime.now().isoformat()}")
            source_id = metadata.get('source_id', 'unknown')
            segmentation_id = metadata.get('segmentation_id', 'manual')
            start = float(metadata.get('start', 0.0))
            end = float(metadata.get('end', 1.0))
            description = metadata.get('description', embedding_text[:50])
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            # Add to database
            success = self.db_manager.add_segment(
                segment_id=segment_id,
                source_id=source_id,
                segmentation_id=segmentation_id,
                start=start,
                end=end,
                description=description,
                embedding_text=embedding_text,
                faiss_index=faiss_id
            )
            
            if success:
                self.osc_handler.send_confirm(f"added segment: {segment_id}")
                logger.info(f"Added segment: {segment_id}")
            else:
                self.osc_handler.send_error("failed to add segment to database")
            
        except Exception as e:
            error_msg = f"add_segment failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_add_preset(self, unused_addr: str, *args):
        """Handle add preset requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            embedding_text = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not embedding_text:
                self.osc_handler.send_error("add_preset requires embedding text")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            # Required fields
            effect_id = metadata.get('effect_id')
            if not effect_id:
                self.osc_handler.send_error("effect_id required in metadata")
                return
            
            parameters = metadata.get('parameters', [])
            description = metadata.get('description', embedding_text[:50])
            
            # Create preset
            preset = {
                "parameters": parameters,
                "description": description,
                "embedding_text": embedding_text
            }
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            preset["FAISS_index"] = faiss_id
            
            # Add to database
            success = self.db_manager.add_preset_to_effect(effect_id, preset)
            
            if success:
                self.osc_handler.send_confirm(f"added preset to effect: {effect_id}")
                logger.info(f"Added preset to effect: {effect_id}")
            else:
                self.osc_handler.send_error("failed to add preset to database")
            
        except Exception as e:
            error_msg = f"add_preset failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_import_csv(self, unused_addr: str, *args):
        """Handle CSV import requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            filepath = parsed.get('arg1', '').strip()
            
            if not filepath:
                self.osc_handler.send_error("import_csv requires filepath")
                return
            
            logger.info(f"Starting CSV import: {filepath}")
            
            # Validate CSV first
            is_valid, issues = self.csv_importer.validate_csv_structure(filepath)
            if not is_valid:
                error_msg = f"CSV validation failed: {'; '.join(issues)}"
                self.osc_handler.send_error(error_msg)
                return
            
            # Import entries
            entries, errors = self.csv_importer.import_csv(filepath, self.db_manager, self.embedding_manager)
            
            result_msg = f"import complete: {len(entries)} entries"
            if errors:
                result_msg += f" ({len(errors)} errors - check logs)"
            
            self.osc_handler.send_confirm(result_msg)
            logger.info(f"CSV import completed: {result_msg}")
            
        except Exception as e:
            error_msg = f"import_csv failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_rebuild_index(self, unused_addr: str, *args):
        """Handle rebuild index requests with hierarchical context."""
        try:
            logger.info("Rebuilding FAISS index from database with hierarchical context...")
            
            # Use text processor for hierarchical embedding text
            stats = self.embedding_manager.rebuild_from_database(
                self.db_manager, 
                self.text_processor
            )
            
            result_msg = f"index rebuilt: {stats['segments_added']} segments, {stats['presets_added']} presets"
            if stats['errors'] > 0:
                result_msg += f" ({stats['errors']} errors)"
            
            self.osc_handler.send_confirm(result_msg)
            logger.info(f"Index rebuild completed: {result_msg}")
            
        except Exception as e:
            error_msg = f"rebuild_index failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_stats(self, unused_addr: str, *args):
        """Handle stats requests."""
        try:
            stats = self.db_manager.get_stats()
            embedding_count = self.embedding_manager.get_total_embeddings()
            
            # Send detailed stats
            stats_msg = (f"Database: {stats.get('recordings', 0)} recordings, "
                        f"{stats.get('segments', 0)} segments, "
                        f"{stats.get('effects', 0)} effects, "
                        f"{stats.get('presets', 0)} presets. "
                        f"FAISS: {embedding_count} embeddings")
            
            self.osc_handler.send_confirm(stats_msg)
            
            # Also send as structured data
            self.osc_handler.client.send_message("/stats_result", [
                stats.get("recordings", 0),
                stats.get("segments", 0),
                stats.get("effects", 0),
                stats.get("presets", 0),
                embedding_count
            ])
            
            logger.info(f"Stats: {stats_msg}")
            
        except Exception as e:
            error_msg = f"stats failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_stop(self, unused_addr: str, *args):
        """Handle stop requests."""
        logger.info("Received stop command")
        self.osc_handler.send_confirm("stopping")
        self.shutdown()
    
    def _shutdown_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
    
    def shutdown(self):
        """Shutdown the server gracefully."""
        if not self.is_running:
            return
        
        logger.info("Shutting down Hibikidō Server...")
        self.is_running = False
        
        try:
            self.osc_handler.close()
            self.db_manager.close()
            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        
        sys.exit(0)


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file {config_file}: {e}")
        return {}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Hibikidō Server')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Load configuration
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Create and start server
    server = HibikidoServer(config)
    
    if not server.initialize():
        logger.error("Failed to initialize server")
        sys.exit(1)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        server.shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        server.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()