#!/usr/bin/env python3
"""
Incantation Server - Main Application (Updated)
===============================================

Music incantation server that maps text descriptions to sounds, effects, and behaviors.
Completely schemaless except for core system fields.

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

from database_manager import DatabaseManager
from embedding_manager import EmbeddingManager
from text_processor import TextProcessor
from csv_importer import CSVImporter
from osc_handler import OSCHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IncantationServer:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or self._default_config()
        
        # Initialize components
        self.db_manager = DatabaseManager(
            uri=self.config['mongodb']['uri'],
            db_name=self.config['mongodb']['database'],
            collection_name=self.config['mongodb']['collection']
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
                'database': 'incantations',
                'collection': 'entries'
            },
            'embedding': {
                'model_name': 'all-MiniLM-L6-v2',
                'index_file': 'incantations.index'
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
        logger.info("Initializing Incantation Server...")
        
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
            'add': self._handle_add,
            'import_csv': self._handle_import_csv,
            'get_by_id': self._handle_get_by_id,
            'soft_delete': self._handle_soft_delete,
            'update_embedding': self._handle_update_embedding,
            'stats': self._handle_stats,
            'list_types': self._handle_list_types,
            'stop': self._handle_stop
        }
        
        self.osc_handler.register_handlers(handlers)
    
    def start(self):
        """Start the server."""
        try:
            logger.info("Starting Incantation Server...")
            
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
        print("🎵 INCANTATION SERVER READY 🎵")
        print("="*70)
        print(f"Database: {stats.get('active', 0)} active entries, "
              f"{stats.get('with_embeddings', 0)} with embeddings")
        print(f"FAISS Index: {self.embedding_manager.get_total_embeddings()} embeddings")
        print(f"Listening: {config['osc']['listen_ip']}:{config['osc']['listen_port']}")
        print(f"Sending: {config['osc']['send_ip']}:{config['osc']['send_port']}")
        print("\nOSC Commands:")
        print("  /search \"incantation text\"     - find matching sounds/fx")
        print("  /add \"text\" \"metadata_json\"  - add new entry")
        print("  /import_csv \"filepath\"         - bulk import from CSV")
        print("  /get_by_id id                  - get specific entry")
        print("  /soft_delete id                - delete entry")
        print("  /update_embedding id \"text\"   - update entry embedding")
        print("  /stats                         - database statistics")
        print("  /list_types                    - available entry types")
        print("  /stop                          - shutdown server")
        print("="*70)
        print()
    
    # OSC Message Handlers
    
    def _handle_search(self, unused_addr: str, *args):
        """Handle search requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            query = parsed.get('arg1', '').strip()
            
            if not query:
                self.osc_handler.send_error("search requires query text")
                return
            
            logger.info(f"Search query: '{query}'")
            
            # Enhance query for better matching
            enhanced_query = self.text_processor.enhance_query(query)
            
            # Search embeddings
            faiss_ids, scores = self.embedding_manager.search(
                enhanced_query, 
                self.config['search']['top_k']
            )
            
            if not faiss_ids:
                self.osc_handler.send_confirm("no matches found")
                return
            
            # Get database entries
            matches = []
            for faiss_id, score in zip(faiss_ids, scores):
                if score < self.config['search']['min_score']:
                    continue
                
                entry = self.db_manager.get_by_faiss_id(faiss_id)
                if entry:
                    # Flexible field extraction
                    match = {
                        'id': entry.get('ID', faiss_id),
                        'score': score
                    }
                    
                    # Try different field names for common fields
                    match['type'] = entry.get('type') or entry.get('Type', 'unknown')
                    match['title'] = entry.get('title') or entry.get('Title', 'untitled')
                    match['file'] = entry.get('file') or entry.get('File', '')
                    match['description'] = entry.get('description') or entry.get('Description', '')
                    match['duration'] = entry.get('duration') or entry.get('Duration', 0)
                    
                    matches.append(match)
            
            self.osc_handler.send_matches(matches)
            logger.info(f"Found {len(matches)} matches for '{query}'")
            
        except Exception as e:
            error_msg = f"search failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_add(self, unused_addr: str, *args):
        """Handle add entry requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            text = parsed.get('arg1', '').strip()
            metadata = parsed.get('arg2', {})
            
            if not text:
                self.osc_handler.send_error("add requires text")
                return
            
            # Handle metadata as string or dict
            if isinstance(metadata, str):
                try:
                    import json
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            # Create entry with flexible fields
            entry = {
                'ID': self.db_manager.get_next_id(),
                'description': text,  # Primary content for embedding
                'created_at': datetime.now(),
                'deleted': False
            }
            
            # Add any metadata fields provided
            entry.update(metadata)
            
            # Create embedding text using description + filename
            entry['embedding_text'] = self.text_processor.create_embedding_sentence(entry)
            
            # Add embedding to FAISS
            faiss_id, is_duplicate = self.embedding_manager.add_embedding(entry['embedding_text'])
            
            if is_duplicate:
                self.osc_handler.send_confirm(f"duplicate detected: FAISS ID {faiss_id}")
                return
            
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            # Add to database
            entry['faiss_id'] = faiss_id
            if self.db_manager.add_entry(entry):
                success_msg = f"added: {entry['ID']} - {text[:50]}"
                self.osc_handler.send_confirm(success_msg)
                logger.info(f"Added entry: {success_msg}")
            else:
                self.osc_handler.send_error("failed to add to database")
            
        except Exception as e:
            error_msg = f"add failed: {e}"
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
            
            # Import entries (will update existing ones)
            entries, errors = self.csv_importer.import_csv(filepath, self.db_manager, self.embedding_manager)
            
            if not entries and not errors:
                self.osc_handler.send_confirm("no changes made - all entries already exist")
                return
            
            result_msg = f"import complete"
            if errors:
                result_msg += f" ({len(errors)} errors - check logs)"
            
            self.osc_handler.send_confirm(result_msg)
            logger.info(f"CSV import completed: {result_msg}")
            
        except Exception as e:
            error_msg = f"import_csv failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_get_by_id(self, unused_addr: str, *args):
        """Handle get by ID requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            
            try:
                entry_id = int(parsed.get('arg1', 0))
            except (ValueError, TypeError):
                self.osc_handler.send_error("get_by_id requires numeric ID")
                return
            
            entry = self.db_manager.get_by_id(entry_id)
            
            if entry:
                # Flexible field extraction
                match = [{
                    'id': entry['ID'],
                    'score': 1.0,  # Perfect match for direct lookup
                    'type': entry.get('type') or entry.get('Type', 'unknown'),
                    'title': entry.get('title') or entry.get('Title', 'untitled'),
                    'file': entry.get('file') or entry.get('File', ''),
                    'description': entry.get('description') or entry.get('Description', ''),
                    'duration': entry.get('duration') or entry.get('Duration', 0)
                }]
                self.osc_handler.send_matches(match)
            else:
                self.osc_handler.send_confirm(f"not found: ID {entry_id}")
            
        except Exception as e:
            error_msg = f"get_by_id failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_soft_delete(self, unused_addr: str, *args):
        """Handle soft delete requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            
            try:
                entry_id = int(parsed.get('arg1', 0))
            except (ValueError, TypeError):
                self.osc_handler.send_error("soft_delete requires numeric ID")
                return
            
            if self.db_manager.soft_delete(entry_id):
                self.osc_handler.send_confirm(f"deleted: ID {entry_id}")
            else:
                self.osc_handler.send_confirm(f"not found: ID {entry_id}")
            
        except Exception as e:
            error_msg = f"soft_delete failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_update_embedding(self, unused_addr: str, *args):
        """Handle update embedding requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            
            try:
                entry_id = int(parsed.get('arg1', 0))
            except (ValueError, TypeError):
                self.osc_handler.send_error("update_embedding requires numeric ID")
                return
            
            new_text = parsed.get('arg2', '').strip()
            if not new_text:
                self.osc_handler.send_error("update_embedding requires new text")
                return
            
            # Check if entry exists
            entry = self.db_manager.get_by_id(entry_id)
            if not entry:
                self.osc_handler.send_confirm(f"not found: ID {entry_id}")
                return
            
            # Create new embedding
            faiss_id, is_duplicate = self.embedding_manager.add_embedding(new_text)
            
            if faiss_id is None:
                self.osc_handler.send_error("failed to create new embedding")
                return
            
            if is_duplicate:
                self.osc_handler.send_confirm(f"duplicate embedding: FAISS ID {faiss_id}")
                return
            
            # Update database
            if self.db_manager.update_embedding_info(entry_id, faiss_id, new_text):
                success_msg = f"updated: ID {entry_id} -> FAISS {faiss_id}"
                self.osc_handler.send_confirm(success_msg)
                logger.info(f"Updated embedding: {success_msg}")
            else:
                self.osc_handler.send_error("failed to update database")
            
        except Exception as e:
            error_msg = f"update_embedding failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_stats(self, unused_addr: str, *args):
        """Handle stats requests."""
        try:
            stats = self.db_manager.get_stats()
            self.osc_handler.send_stats(stats)
            
            # Log detailed stats
            logger.info(f"Stats - Total: {stats.get('total', 0)}, "
                       f"Active: {stats.get('active', 0)}, "
                       f"With embeddings: {stats.get('with_embeddings', 0)}")
            
            type_breakdown = stats.get('type_breakdown', {})
            for entry_type, count in type_breakdown.items():
                logger.info(f"  {entry_type}: {count}")
            
        except Exception as e:
            error_msg = f"stats failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_list_types(self, unused_addr: str, *args):
        """Handle list types requests."""
        try:
            types = self.db_manager.get_types()
            self.osc_handler.send_types(types)
            logger.info(f"Available types: {types}")
            
        except Exception as e:
            error_msg = f"list_types failed: {e}"
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
        
        logger.info("Shutting down Incantation Server...")
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
    parser = argparse.ArgumentParser(description='Incantation Server')
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
    server = IncantationServer(config)
    
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