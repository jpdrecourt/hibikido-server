#!/usr/bin/env python3
"""
Hibikidō Server - Main Application
==================================

Music server that maps text descriptions to sounds and effects using semantic search.
Supports hierarchical database with recordings, segments, effects, and performances.

Usage:
    python main_server.py [--config config.json]

Dependencies:
    pip install sentence-transformers python-osc faiss-cpu torch pymongo
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
            logger.error("Hibikidō Server: Failed to connect to database")
            return False
        
        # Initialize embedding system
        if not self.embedding_manager.initialize():
            logger.error("Hibikidō Server: Failed to initialize embedding system")
            return False
        
        # Initialize OSC
        if not self.osc_handler.initialize():
            logger.error("Hibikidō Server: Failed to initialize OSC")
            return False
        
        # Register OSC handlers
        self._register_osc_handlers()
        
        logger.info("Hibikidō Server: All components initialized successfully")
        return True
    
    def _register_osc_handlers(self):
        """Register OSC message handlers."""
        handlers = {
            'search': self._handle_search,
            'add_recording': self._handle_add_recording,
            'add_effect': self._handle_add_effect,
            'add_segment': self._handle_add_segment,
            'add_preset': self._handle_add_preset,
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
                logger.error("Hibikidō Server: Failed to start OSC server")
                return
            
            self.is_running = True
            
            # Send ready signal
            self.osc_handler.send_ready()
            
            # Print startup banner
            self._print_banner()
            
            # Start serving
            logger.info("Hibikidō Server: Ready - waiting for OSC messages...")
            server.serve_forever()
            
        except Exception as e:
            logger.error(f"Hibikidō Server error: {e}")
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
        print("  /add_recording \"path\" metadata  - add new recording with auto-segment")
        print("  /add_effect \"path\" metadata     - add new effect with default preset")
        print("  /add_segment \"text\" metadata    - add new segment")
        print("  /add_preset \"text\" metadata     - add new effect preset")
        print("  /rebuild_index                   - rebuild FAISS index from database")
        print("  /stats                           - database statistics")
        print("  /stop                            - shutdown server")
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
            
            logger.info(f"Hibikidō Server: Search query: '{query}'")
            
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
            logger.info(f"Hibikidō Server: Search '{query}' returned {len(results)} results")
            
        except Exception as e:
            error_msg = f"search failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)
    
    def _send_search_results(self, results: List[Dict[str, Any]]):
        """Send search results as simplified, uniform data via OSC."""
        try:
            # Send each result as separate OSC message with uniform format
            for i, result in enumerate(results):
                collection = result["collection"]
                document = result["document"]
                score = result["score"]
                
                # Extract common fields
                if collection == "segments":
                    path = document.get("source_path", "")
                    start = document.get("start", 0.0)
                    end = document.get("end", 1.0)
                    parameters = []  # Empty for segments
                else:  # presets
                    path = document.get("effect_path", "")
                    start = 0.0  # Default for presets
                    end = 0.0    # Default for presets  
                    parameters = document.get("parameters", [])
                
                # Create description from embedding text using reverse processing
                embedding_text = document.get("embedding_text", "")
                description = self._create_display_description(embedding_text)
                
                # Send uniform result format
                self.osc_handler.client.send_message("/result", [
                    i,                    # Result index
                    collection,           # "segments" or "presets"
                    float(score),         # Similarity score
                    str(path),           # File path
                    str(description),    # Human-readable description
                    float(start),        # Start time (0.0 for presets)
                    float(end),          # End time (0.0 for presets)
                    str(json.dumps(parameters))  # Parameters as JSON string ([] for segments)
                ])
            
            # Send completion message
            self.osc_handler.client.send_message("/search_complete", len(results))
            
        except Exception as e:
            logger.error(f"Hibikidō Server: Failed to send search results: {e}")
            self.osc_handler.send_error(f"failed to send results: {e}")
    
    def _handle_add_recording(self, unused_addr: str, *args):
        """Handle add recording requests - creates recording + auto-segment."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            path = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not path:
                self.osc_handler.send_error("add_recording requires audio file path")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            description = metadata.get('description', f"Recording: {path}")
            
            # Add recording to database (path-based, rejects duplicates)
            success = self.db_manager.add_recording(
                path=path,
                description=description
            )
            
            if not success:
                self.osc_handler.send_error(f"recording already exists or failed to add: {path}")
                return
            
            # Auto-create full-length segment
            segment_description = f"Full recording: {description}"
            
            # Create embedding for segment with hierarchical context
            segment_embedding_text = self.text_processor.create_segment_embedding_text(
                segment={'description': segment_description},
                recording={'description': description, 'path': path},
                segmentation={'description': 'Auto-generated full recording segment'}
            )
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(segment_embedding_text)
            if faiss_id is None:
                logger.warning(f"Hibikidō Server: Failed to create embedding for auto-segment")
            
            # Add auto-segment (references recording by path)
            segment_success = self.db_manager.add_segment(
                source_path=path,
                segmentation_id="auto_full",
                start=0.0,
                end=1.0,
                description=segment_description,
                embedding_text=segment_embedding_text,
                faiss_index=faiss_id
            )
            
            if segment_success:
                self.osc_handler.send_confirm(f"added recording: {path} with auto-segment")
                logger.info(f"Hibikidō Server: Added recording: {path} with auto-segment at FAISS {faiss_id}")
            else:
                self.osc_handler.send_confirm(f"added recording: {path} (segment creation failed)")
                logger.warning(f"Hibikidō Server: Recording added but auto-segment failed: {path}")
            
        except Exception as e:
            error_msg = f"add_recording failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)

    def _handle_add_effect(self, unused_addr: str, *args):
        """Handle add effect requests - creates effect + default preset."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            path = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not path:
                self.osc_handler.send_error("add_effect requires effect path")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            # Extract name from path if not provided
            name = metadata.get('name', path.split('/')[-1].split('.')[0])
            description = metadata.get('description', f"Effect: {name}")
            
            # Add effect to database (path-based, rejects duplicates)
            success = self.db_manager.add_effect(
                path=path,
                name=name,
                description=description
            )
            
            if not success:
                self.osc_handler.send_error(f"effect already exists or failed to add: {path}")
                return
            
            # Create default preset
            preset_description = f"Default preset: {description}"
            preset_embedding_text = self.text_processor.create_preset_embedding_text(
                preset={'description': preset_description},
                effect={'description': description, 'name': name, 'path': path}
            )
            
            # Add embedding for default preset
            faiss_id = self.embedding_manager.add_embedding(preset_embedding_text)
            if faiss_id is None:
                logger.warning(f"Hibikidō Server: Failed to create embedding for default preset")
            
            # Add default preset (separate collection, references effect by path)
            preset_success = self.db_manager.add_preset(
                effect_path=path,
                parameters=[],
                description=preset_description,
                embedding_text=preset_embedding_text,
                faiss_index=faiss_id
            )
            
            if preset_success:
                self.osc_handler.send_confirm(f"added effect: {path} with default preset")
                logger.info(f"Hibikidō Server: Added effect: {path} with default preset at FAISS {faiss_id}")
            else:
                self.osc_handler.send_confirm(f"added effect: {path} (preset creation failed)")
                logger.warning(f"Hibikidō Server: Effect added but default preset failed: {path}")
            
        except Exception as e:
            error_msg = f"add_effect failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)

    def _handle_add_segment(self, unused_addr: str, *args):
        """Handle add segment requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            description = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not description:
                self.osc_handler.send_error("add_segment requires description")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            # Required fields
            source_path = metadata.get('source_path')
            if not source_path:
                self.osc_handler.send_error("source_path required in metadata")
                return
            
            segmentation_id = metadata.get('segmentation_id', 'manual')
            start = float(metadata.get('start', 0.0))
            end = float(metadata.get('end', 1.0))

            # Validate normalized values (0.0 to 1.0)
            if not (0.0 <= start <= 1.0):
                self.osc_handler.send_error(f"start must be between 0.0 and 1.0, got {start}")
                return

            if not (0.0 <= end <= 1.0):
                self.osc_handler.send_error(f"end must be between 0.0 and 1.0, got {end}")
                return

            if start >= end:
                self.osc_handler.send_error(f"start ({start}) must be less than end ({end})")
                return
            
            # Verify recording exists
            recording = self.db_manager.get_recording_by_path(source_path)
            if not recording:
                self.osc_handler.send_error(f"recording not found: {source_path}")
                return
            
            # Create hierarchical embedding text
            embedding_text = self.text_processor.create_segment_embedding_text(
                segment={'description': description},
                recording=recording,
                segmentation={'description': f'Manual segmentation: {segmentation_id}'}
            )
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            # Add to database
            success = self.db_manager.add_segment(
                source_path=source_path,
                segmentation_id=segmentation_id,
                start=start,
                end=end,
                description=description,
                embedding_text=embedding_text,
                faiss_index=faiss_id
            )
            
            if success:
                self.osc_handler.send_confirm(f"added segment for {source_path} [{start}-{end}]")
                logger.info(f"Hibikidō Server: Added segment for {source_path} at FAISS {faiss_id}")
            else:
                self.osc_handler.send_error("failed to add segment to database")
            
        except Exception as e:
            error_msg = f"add_segment failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)

    def _handle_add_preset(self, unused_addr: str, *args):
        """Handle add preset requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            description = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not description:
                self.osc_handler.send_error("add_preset requires description")
                return
            
            # Parse metadata
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            # Required fields
            effect_path = metadata.get('effect_path')
            if not effect_path:
                self.osc_handler.send_error("effect_path required in metadata")
                return
            
            parameters = metadata.get('parameters', [])
            
            # Verify effect exists
            effect = self.db_manager.get_effect_by_path(effect_path)
            if not effect:
                self.osc_handler.send_error(f"effect not found: {effect_path}")
                return
            
            # Create hierarchical embedding text
            embedding_text = self.text_processor.create_preset_embedding_text(
                preset={'description': description, 'parameters': parameters},
                effect=effect
            )
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            # Add to database (separate presets collection)
            success = self.db_manager.add_preset(
                effect_path=effect_path,
                parameters=parameters,
                description=description,
                embedding_text=embedding_text,
                faiss_index=faiss_id
            )
            
            if success:
                self.osc_handler.send_confirm(f"added preset for {effect_path}")
                logger.info(f"Hibikidō Server: Added preset for {effect_path} at FAISS {faiss_id}")
            else:
                self.osc_handler.send_error("failed to add preset to database")
            
        except Exception as e:
            error_msg = f"add_preset failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)
    
    def _handle_rebuild_index(self, unused_addr: str, *args):
        """Handle rebuild index requests with hierarchical context."""
        try:
            logger.info("Hibikidō Server: Rebuilding FAISS index from database with hierarchical context...")
            
            # Use text processor for hierarchical embedding text
            stats = self.embedding_manager.rebuild_from_database(
                self.db_manager, 
                self.text_processor
            )
            
            result_msg = f"index rebuilt: {stats['segments_added']} segments, {stats['presets_added']} presets"
            if stats['errors'] > 0:
                result_msg += f" ({stats['errors']} errors)"
            
            self.osc_handler.send_confirm(result_msg)
            logger.info(f"Hibikidō Server: Index rebuild completed: {result_msg}")
            
        except Exception as e:
            error_msg = f"rebuild_index failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
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
            
            logger.info(f"Hibikidō Server: Stats: {stats_msg}")
            
        except Exception as e:
            error_msg = f"stats failed: {e}"
            logger.error(f"Hibikidō Server: {error_msg}")
            self.osc_handler.send_error(error_msg)
            
    def _create_display_description(self, embedding_text: str) -> str:
        """Create human-readable description from embedding text."""
        try:
            if not embedding_text:
                return "untitled"
            
            # Simple reverse processing - clean up the embedding text for display
            # Remove common embedding artifacts and make it more natural
            
            words = embedding_text.split()
            
            # If we have spaCy, try to make it more natural
            if hasattr(self, 'text_processor') and self.text_processor.nlp and self.text_processor.spacy_working:
                try:
                    doc = self.text_processor.nlp(embedding_text)
                    
                    # Extract key phrases and entities
                    noun_phrases = []
                    for chunk in doc.noun_chunks:
                        if len(chunk.text.strip()) > 2:
                            noun_phrases.append(chunk.text.strip())
                    
                    if noun_phrases:
                        # Use the first good noun phrase as description
                        description = noun_phrases[0]
                        # Add additional context if short
                        if len(description.split()) < 3 and len(noun_phrases) > 1:
                            description = f"{description} {noun_phrases[1]}"
                        return description[:60]  # Limit length
                    
                except Exception:
                    pass  # Fall back to simple processing
            
            # Simple fallback processing
            # Take first 4-6 meaningful words and clean them up
            meaningful_words = []
            for word in words[:8]:  # Look at first 8 words
                word = word.strip().lower()
                if len(word) > 2 and word not in ['the', 'and', 'for', 'with', 'this', 'that']:
                    meaningful_words.append(word)
                if len(meaningful_words) >= 6:
                    break
            
            if meaningful_words:
                description = " ".join(meaningful_words[:6])
                # Capitalize first word
                if description:
                    description = description[0].upper() + description[1:]
                return description[:60]  # Limit length
            
            # Ultimate fallback
            return embedding_text[:40].strip() or "untitled"
            
        except Exception as e:
            logger.warning(f"Hibikidō Server: Failed to create display description: {e}")
            return "untitled"
    
    def _handle_stop(self, unused_addr: str, *args):
        """Handle stop requests."""
        logger.info("Hibikidō Server: Received stop command")
        self.osc_handler.send_confirm("stopping")
        self.shutdown()
    
    def _shutdown_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Hibikidō Server: Received signal {signum}, shutting down...")
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
            logger.info("Hibikidō Server: Shutdown complete")
        except Exception as e:
            logger.error(f"Hibikidō Server: Error during shutdown: {e}")
        
        sys.exit(0)


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Hibikidō Server: Failed to load config file {config_file}: {e}")
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
        logger.error("Hibikidō Server: Failed to initialize server")
        sys.exit(1)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Hibikidō Server: Interrupted by user")
        server.shutdown()
    except Exception as e:
        logger.error(f"Hibikidō Server: Unexpected error: {e}")
        server.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()