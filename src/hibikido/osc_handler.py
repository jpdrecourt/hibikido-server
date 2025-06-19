"""
OSC Handler for Hibikidō Server
===============================

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
            # Input addresses
            'search': '/search',
            'add_recording': '/add_recording',
            'add_effect': '/add_effect', 
            'add_segment': '/add_segment',
            'add_preset': '/add_preset',
            'rebuild_index': '/rebuild_index',
            'stats': '/stats',
            'stop': '/stop',
            
            # Output addresses
            'result': '/result',
            'search_complete': '/search_complete',
            'confirm': '/confirm',
            'stats_result': '/stats_result',
            'error': '/error'
        }
    
    def initialize(self) -> bool:
        """Initialize OSC client and server."""
        try:
            # Setup client for sending messages
            self.client = SimpleUDPClient(self.send_ip, self.send_port)
            
            # Setup dispatcher for routing incoming messages
            self.dispatcher = Dispatcher()
            
            logger.info(f"Hibikidō OSC: Initialized - listening: {self.listen_ip}:{self.listen_port}, "
                       f"sending: {self.send_ip}:{self.send_port}")
            return True
            
        except Exception as e:
            logger.error(f"Hibikidō OSC: Failed to initialize: {e}")
            return False
    
    def register_handlers(self, handlers: Dict[str, callable]):
        """Register message handlers with the dispatcher."""
        for address_name, handler_func in handlers.items():
            if address_name in self.addresses:
                osc_address = self.addresses[address_name]
                self.dispatcher.map(osc_address, handler_func)
                logger.debug(f"Hibikidō OSC: Registered handler for {osc_address}")
            else:
                logger.warning(f"Hibikidō OSC: Unknown OSC address: {address_name}")
    
    def start_server(self) -> BlockingOSCUDPServer:
        """Start the OSC server."""
        try:
            self.server = BlockingOSCUDPServer(
                (self.listen_ip, self.listen_port), 
                self.dispatcher
            )
            logger.info(f"Hibikidō OSC: Server started on {self.listen_ip}:{self.listen_port}")
            return self.server
            
        except Exception as e:
            logger.error(f"Hibikidō OSC: Failed to start server: {e}")
            return None
    
    def send_confirm(self, message: str):
        """Send confirmation message."""
        try:
            self.client.send_message(self.addresses['confirm'], message)
            logger.debug(f"Hibikidō OSC: Sent confirmation: {message}")
        except Exception as e:
            logger.error(f"Hibikidō OSC: Failed to send confirmation: {e}")
    
    def send_error(self, error_message: str):
        """Send error message."""
        try:
            self.client.send_message(self.addresses['error'], error_message)
            logger.error(f"Hibikidō OSC: Sent error: {error_message}")
        except Exception as e:
            logger.error(f"Hibikidō OSC: Failed to send error message: {e}")
    
    def send_ready(self):
        """Send ready signal."""
        self.send_confirm("hibikido_server_ready")
    
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
                logger.info("Hibikidō OSC: Server closed")
        except Exception as e:
            logger.error(f"Hibikidō OSC: Error closing server: {e}")