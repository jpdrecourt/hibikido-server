"""
OSC Handler for Incantation Server
=================================

Handles all OSC communication and message routing.
"""

import json
from typing import List, Dict, Any
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_server import BlockingOSCUDPServer
from pythonosc.udp_client import SimpleUDPClient
import logging

logger = logging.getLogger(__name__)

class OSCHandler:
    def __init__(self, listen_ip: str = "127.0.0.1", listen_port: int = 9000,
                 send_ip: str = "127.0.0.1", send_port: int = 9001):
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.send_ip = send_ip
        self.send_port = send_port
        
        self.client = None
        self.server = None
        self.dispatcher = None
        
        # OSC Address definitions
        self.addresses = {
            'search': '/search',
            'add': '/add',
            'import_csv': '/import_csv',
            'get_by_id': '/get_by_id',
            'soft_delete': '/soft_delete',
            'update_embedding': '/update_embedding',
            'stats': '/stats',
            'list_types': '/list_types',
            'stop': '/stop',
            
            # Output addresses
            'matches': '/matches',
            'confirm': '/confirm',
            'stats_result': '/stats_result',
            'types': '/types',
            'error': '/error'
        }
    
    def initialize(self) -> bool:
        """Initialize OSC client and server."""
        try:
            # Setup client for sending messages
            self.client = SimpleUDPClient(self.send_ip, self.send_port)
            
            # Setup dispatcher for routing incoming messages
            self.dispatcher = Dispatcher()
            
            logger.info(f"OSC initialized - listening: {self.listen_ip}:{self.listen_port}, "
                       f"sending: {self.send_ip}:{self.send_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize OSC: {e}")
            return False
    
    def register_handlers(self, handlers: Dict[str, callable]):
        """Register message handlers with the dispatcher."""
        for address_name, handler_func in handlers.items():
            if address_name in self.addresses:
                osc_address = self.addresses[address_name]
                self.dispatcher.map(osc_address, handler_func)
                logger.debug(f"Registered handler for {osc_address}")
            else:
                logger.warning(f"Unknown OSC address: {address_name}")
    
    def start_server(self) -> BlockingOSCUDPServer:
        """Start the OSC server."""
        try:
            self.server = BlockingOSCUDPServer(
                (self.listen_ip, self.listen_port), 
                self.dispatcher
            )
            logger.info(f"OSC server started on {self.listen_ip}:{self.listen_port}")
            return self.server
            
        except Exception as e:
            logger.error(f"Failed to start OSC server: {e}")
            return None
    
    def send_matches(self, matches: List[Dict[str, Any]]):
        """Send search matches to client."""
        try:
            if not matches:
                self.send_confirm("no matches found")
                return
            
            # Flatten matches for OSC transmission: [id1, type1, title1, file1, score1, ...]
            flat_data = []
            for match in matches:
                flat_data.extend([
                    match.get("id", 0),
                    match.get("type", "unknown"),
                    match.get("title", "untitled"),
                    match.get("file", ""),
                    match.get("score", 0.0)
                ])
            
            self.client.send_message(self.addresses['matches'], flat_data)
            logger.debug(f"Sent {len(matches)} matches")
            
        except Exception as e:
            logger.error(f"Failed to send matches: {e}")
            self.send_error(f"send_matches_failed: {e}")
    
    def send_confirm(self, message: str):
        """Send confirmation message."""
        try:
            self.client.send_message(self.addresses['confirm'], message)
            logger.debug(f"Sent confirmation: {message}")
        except Exception as e:
            logger.error(f"Failed to send confirmation: {e}")
    
    def send_error(self, error_message: str):
        """Send error message."""
        try:
            self.client.send_message(self.addresses['error'], error_message)
            logger.error(f"Sent error: {error_message}")
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")
    
    def send_stats(self, stats: Dict[str, Any]):
        """Send database statistics."""
        try:
            stats_array = [
                stats.get("total", 0),
                stats.get("active", 0),
                stats.get("deleted", 0),
                stats.get("with_embeddings", 0)
            ]
            self.client.send_message(self.addresses['stats_result'], stats_array)
            logger.debug(f"Sent stats: {stats_array}")
            
        except Exception as e:
            logger.error(f"Failed to send stats: {e}")
            self.send_error(f"send_stats_failed: {e}")
    
    def send_types(self, types: List[str]):
        """Send available types list."""
        try:
            self.client.send_message(self.addresses['types'], types)
            logger.debug(f"Sent {len(types)} types")
            
        except Exception as e:
            logger.error(f"Failed to send types: {e}")
            self.send_error(f"send_types_failed: {e}")
    
    def send_ready(self):
        """Send ready signal."""
        self.send_confirm("incantation_server_ready")
    
    @staticmethod
    def parse_args(*args) -> Dict[str, Any]:
        """Parse OSC arguments into a clean dictionary."""
        parsed = {}
        
        if len(args) >= 1:
            parsed['arg1'] = str(args[0]) if args[0] is not None else ""
        if len(args) >= 2:
            parsed['arg2'] = str(args[1]) if args[1] is not None else ""
        if len(args) >= 3:
            # Try to parse third argument as JSON
            try:
                parsed['arg3'] = json.loads(str(args[2])) if args[2] else {}
            except (json.JSONDecodeError, TypeError):
                parsed['arg3'] = str(args[2]) if args[2] is not None else ""
        
        # Add all remaining args
        if len(args) > 3:
            parsed['extra_args'] = [str(arg) for arg in args[3:]]
        
        return parsed
    
    def close(self):
        """Close OSC connections."""
        try:
            if self.server:
                self.server.server_close()
                logger.info("OSC server closed")
        except Exception as e:
            logger.error(f"Error closing OSC server: {e}")