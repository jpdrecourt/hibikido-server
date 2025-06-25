#!/usr/bin/env python3
"""
Hibikidō Server - Main Application (Invocation Protocol)
========================================================

Music server using invocation paradigm - sounds manifest when the cosmos permits.
All search results queue through orchestrator, no completion signals.
"""

import signal
import sys
import json
import argparse
import logging
import threading
import time
from typing import Dict, Any, List
from datetime import datetime

from .database_manager import HibikidoDatabase
from .embedding_manager import EmbeddingManager
from .text_processor import TextProcessor
from .osc_handler import OSCHandler
from .orchestrator import Orchestrator

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
        
        # Initialize orchestrator
        self.orchestrator = Orchestrator(
            overlap_threshold=self.config['orchestrator']['overlap_threshold'],
            time_precision=self.config['orchestrator']['time_precision']
        )
        
        self.is_running = False
        self.update_thread = None
    
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
            },
            'orchestrator': {
                'overlap_threshold': 0.2,  # 20%
                'time_precision': 0.1      # 100ms
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
        
        # Setup orchestrator callback
        self.orchestrator.set_manifest_callback(self.osc_handler.send_manifest)
        
        # Register OSC handlers
        self._register_osc_handlers()
        
        logger.info("All components initialized successfully")
        return True
    
    def _register_osc_handlers(self):
        """Register OSC message handlers."""
        handlers = {
            'invoke': self._handle_invoke,  # Changed from 'search'
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
            
            # Start orchestrator update thread
            self._start_orchestrator_updates()
            
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
            logger.info("Ready - waiting for invocations...")
            server.serve_forever()
            
        except Exception as e:
            logger.error(f"Server error: {e}")
            self.shutdown()
    
    def _start_orchestrator_updates(self):
        """Start background thread for orchestrator updates."""
        def update_loop():
            while self.is_running:
                try:
                    self.orchestrator.update()
                    time.sleep(self.config['orchestrator']['time_precision'])
                except Exception as e:
                    logger.error(f"Orchestrator update error: {e}")
        
        self.update_thread = threading.Thread(target=update_loop, daemon=True)
        self.update_thread.start()
        logger.info("Orchestrator update thread started")
    
    def _print_banner(self):
        """Print startup banner with information."""
        config = self.config
        stats = self.db_manager.get_stats()
        orch_stats = self.orchestrator.get_stats()
        
        print("\n" + "="*70)
        print("🎵 HIBIKIDŌ SERVER READY 🎵")
        print("="*70)
        print(f"Database: {stats.get('segments', 0)} segments, "
              f"{stats.get('presets', 0)} presets, "
              f"{stats.get('total_searchable_items', 0)} searchable")
        print(f"FAISS Index: {self.embedding_manager.get_total_embeddings()} embeddings")
        print(f"Orchestrator: {orch_stats['overlap_threshold']*100:.0f}% overlap threshold, "
              f"{orch_stats['time_precision']*1000:.0f}ms precision")
        print(f"Listening: {config['osc']['listen_ip']}:{config['osc']['listen_port']}")
        print(f"Sending: {config['osc']['send_ip']}:{config['osc']['send_port']}")
        print("\nOSC Commands:")
        print("  /invoke \"incantation\"           - semantic invocation → manifestations")
        print("  /add_recording \"path\" metadata  - add new recording with auto-segment")
        print("  /add_effect \"path\" metadata     - add new effect with default preset")
        print("  /add_segment \"text\" metadata    - add new segment")
        print("  /add_preset \"text\" metadata     - add new effect preset")
        print("  /rebuild_index                   - rebuild FAISS index from database")
        print("  /stats                           - database and orchestrator statistics")
        print("  /stop                            - shutdown server")
        print("="*70)
        print()
    
    # OSC Message Handlers
    
    def _handle_invoke(self, unused_addr: str, *args):
        """Handle invocation requests - queue all results for manifestation."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            incantation = parsed.get('arg1', '').strip()
            
            if not incantation:
                self.osc_handler.send_error("invoke requires incantation text")
                return
            
            logger.info(f"Invocation: '{incantation}'")
            
            # Search with MongoDB lookups
            results = self.embedding_manager.search(
                incantation, 
                self.config['search']['top_k'],
                db_manager=self.db_manager
            )
            
            if not results:
                self.osc_handler.send_confirm("no resonance found")
                return
            
            # Filter to segments only (MVP requirement)
            segment_results = [r for r in results if r["collection"] == "segments"]
            
            if not segment_results:
                self.osc_handler.send_confirm("no segment resonance found")
                return
            
            # Queue ALL results for orchestrator processing
            queued_count = 0
            for i, result in enumerate(segment_results):
                document = result["document"]
                
                # Extract metadata for orchestrator
                freq_low = document.get("freq_low", 200)
                freq_high = document.get("freq_high", 2000)
                duration = document.get("duration", 1.0)
                sound_id = str(document.get("_id", "unknown"))
                
                # Prepare manifestation data
                manifestation_data = {
                    "index": i,
                    "collection": "segments",
                    "score": float(result["score"]),
                    "path": str(document.get("source_path", "")),
                    "description": self._create_display_description(
                        document.get("embedding_text", "")
                    ),
                    "start": float(document.get("start", 0.0)),
                    "end": float(document.get("end", 1.0)),
                    "parameters": "[]",  # Empty for segments
                    "sound_id": sound_id,
                    "freq_low": freq_low,
                    "freq_high": freq_high,
                    "duration": duration
                }
                
                # Queue for orchestrator (no immediate manifestation)
                if self.orchestrator.queue_manifestation(manifestation_data):
                    queued_count += 1
            
            # Simple confirmation - no completion signal
            self.osc_handler.send_confirm(f"invoked: {queued_count} resonances queued")
            logger.info(f"Invocation '{incantation}' queued {queued_count} manifestations")
            
        except Exception as e:
            error_msg = f"invocation failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _create_display_description(self, embedding_text: str) -> str:
        """Create human-readable description from embedding text."""
        try:
            if not embedding_text:
                return "untitled"
            
            # Simple processing for performance
            words = embedding_text.split()
            
            # Take first few meaningful words
            meaningful_words = []
            for word in words[:8]:
                word = word.strip().lower()
                if len(word) > 2 and word not in ['the', 'and', 'for', 'with']:
                    meaningful_words.append(word)
                if len(meaningful_words) >= 4:
                    break
            
            if meaningful_words:
                description = " ".join(meaningful_words[:4])
                # Capitalize first word
                if description:
                    description = description[0].upper() + description[1:]
                return description[:50]
            
            return embedding_text[:30].strip() or "untitled"
            
        except Exception as e:
            logger.warning(f"Failed to create display description: {e}")
            return "untitled"
    
    def _handle_stats(self, unused_addr: str, *args):
        """Handle stats requests (includes orchestrator)."""
        try:
            stats = self.db_manager.get_stats()
            embedding_count = self.embedding_manager.get_total_embeddings()
            orch_stats = self.orchestrator.get_stats()
            
            # Send detailed stats
            stats_msg = (f"Database: {stats.get('recordings', 0)} recordings, "
                        f"{stats.get('segments', 0)} segments, "
                        f"{stats.get('effects', 0)} effects, "
                        f"{stats.get('presets', 0)} presets. "
                        f"FAISS: {embedding_count} embeddings. "
                        f"Orchestrator: {orch_stats['active_niches']} active, "
                        f"{orch_stats['queued_requests']} queued")
            
            self.osc_handler.send_confirm(stats_msg)
            
            # Also send as structured data
            self.osc_handler.client.send_message("/stats_result", [
                stats.get("recordings", 0),
                stats.get("segments", 0),
                stats.get("effects", 0),
                stats.get("presets", 0),
                embedding_count,
                orch_stats["active_niches"],
                orch_stats["queued_requests"]
            ])
            
            logger.info(f"Stats: {stats_msg}")
            
        except Exception as e:
            error_msg = f"stats failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    # Keep existing add handlers unchanged - only OSC protocol changes
    def _handle_add_recording(self, unused_addr: str, *args):
        """Handle add recording requests."""
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
            
            # Add recording to database
            success = self.db_manager.add_recording(
                path=path,
                description=description
            )
            
            if not success:
                self.osc_handler.send_error(f"recording already exists or failed to add: {path}")
                return
            
            # Auto-create full-length segment
            segment_description = f"Full recording: {description}"
            
            segment_embedding_text = self.text_processor.create_segment_embedding_text(
                segment={'description': segment_description},
                recording={'description': description, 'path': path},
                segmentation={'description': 'Auto-generated full recording segment'}
            )
            
            # Add embedding
            faiss_id = self.embedding_manager.add_embedding(segment_embedding_text)
            
            # Add auto-segment
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
                logger.info(f"Added recording: {path} with auto-segment at FAISS {faiss_id}")
            else:
                self.osc_handler.send_confirm(f"added recording: {path} (segment creation failed)")
                
        except Exception as e:
            error_msg = f"add_recording failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)

    def _handle_add_effect(self, unused_addr: str, *args):
        """Handle add effect requests."""
        try:
            parsed = self.osc_handler.parse_args(*args)
            path = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not path:
                self.osc_handler.send_error("add_effect requires effect path")
                return
            
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            name = metadata.get('name', path.split('/')[-1].split('.')[0])
            description = metadata.get('description', f"Effect: {name}")
            
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
            
            faiss_id = self.embedding_manager.add_embedding(preset_embedding_text)
            
            preset_success = self.db_manager.add_preset(
                effect_path=path,
                parameters=[],
                description=preset_description,
                embedding_text=preset_embedding_text,
                faiss_index=faiss_id
            )
            
            if preset_success:
                self.osc_handler.send_confirm(f"added effect: {path} with default preset")
                logger.info(f"Added effect: {path} with default preset at FAISS {faiss_id}")
            else:
                self.osc_handler.send_confirm(f"added effect: {path} (preset creation failed)")
                
        except Exception as e:
            error_msg = f"add_effect failed: {e}"
            logger.error(error_msg)
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
            
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            source_path = metadata.get('source_path')
            if not source_path:
                self.osc_handler.send_error("source_path required in metadata")
                return
            
            segmentation_id = metadata.get('segmentation_id', 'manual')
            start = float(metadata.get('start', 0.0))
            end = float(metadata.get('end', 1.0))

            if not (0.0 <= start <= 1.0) or not (0.0 <= end <= 1.0) or start >= end:
                self.osc_handler.send_error("invalid start/end values (must be 0.0-1.0)")
                return
            
            recording = self.db_manager.get_recording_by_path(source_path)
            if not recording:
                self.osc_handler.send_error(f"recording not found: {source_path}")
                return
            
            embedding_text = self.text_processor.create_segment_embedding_text(
                segment={'description': description},
                recording=recording,
                segmentation={'description': f'Manual segmentation: {segmentation_id}'}
            )
            
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
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
                logger.info(f"Added segment for {source_path} at FAISS {faiss_id}")
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
            description = parsed.get('arg1', '').strip()
            metadata_str = parsed.get('arg2', '{}')
            
            if not description:
                self.osc_handler.send_error("add_preset requires description")
                return
            
            try:
                metadata = json.loads(metadata_str) if metadata_str != '{}' else {}
            except json.JSONDecodeError:
                self.osc_handler.send_error("invalid metadata JSON")
                return
            
            effect_path = metadata.get('effect_path')
            if not effect_path:
                self.osc_handler.send_error("effect_path required in metadata")
                return
            
            parameters = metadata.get('parameters', [])
            
            effect = self.db_manager.get_effect_by_path(effect_path)
            if not effect:
                self.osc_handler.send_error(f"effect not found: {effect_path}")
                return
            
            embedding_text = self.text_processor.create_preset_embedding_text(
                preset={'description': description, 'parameters': parameters},
                effect=effect
            )
            
            faiss_id = self.embedding_manager.add_embedding(embedding_text)
            if faiss_id is None:
                self.osc_handler.send_error("failed to create embedding")
                return
            
            success = self.db_manager.add_preset(
                effect_path=effect_path,
                parameters=parameters,
                description=description,
                embedding_text=embedding_text,
                faiss_index=faiss_id
            )
            
            if success:
                self.osc_handler.send_confirm(f"added preset for {effect_path}")
                logger.info(f"Added preset for {effect_path} at FAISS {faiss_id}")
            else:
                self.osc_handler.send_error("failed to add preset to database")
                
        except Exception as e:
            error_msg = f"add_preset failed: {e}"
            logger.error(error_msg)
            self.osc_handler.send_error(error_msg)
    
    def _handle_rebuild_index(self, unused_addr: str, *args):
        """Handle rebuild index requests."""
        try:
            logger.info("Rebuilding FAISS index from database...")
            
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