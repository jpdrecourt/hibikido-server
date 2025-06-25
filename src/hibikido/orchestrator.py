"""
Orchestrator for Hibikidō (Enhanced for Manifestation Protocol)
===============================================================

Manages time-frequency niches and sends manifestations when sounds can play.
All results go through the queue - orchestrator decides when to manifest.
"""

import time
import math
import json
from typing import Dict, List, Any, Optional, Callable
import logging

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self, overlap_threshold: float = 0.2, time_precision: float = 0.1):
        """
        Initialize orchestrator.
        
        Args:
            overlap_threshold: Maximum allowed logarithmic frequency overlap (0.2 = 20%)
            time_precision: Time precision in seconds (0.1 = 100ms)
        """
        self.overlap_threshold = overlap_threshold
        self.time_precision = time_precision
        
        # Active niches: list of dicts with sound_id, start_time, end_time, freq_low, freq_high
        self.active_niches = []
        
        # Queue for manifestations: list of (manifestation_data, request_time)
        self.queue = []
        
        # Callback for sending manifestations
        self.manifest_callback = None
        
        logger.info(f"Orchestrator initialized: {overlap_threshold*100:.0f}% overlap threshold, "
                   f"{time_precision*1000:.0f}ms precision")
    
    def set_manifest_callback(self, callback: Callable):
        """Set callback function for sending manifestations."""
        self.manifest_callback = callback
    
    def queue_manifestation(self, manifestation_data: Dict[str, Any]) -> bool:
        """
        Queue a manifestation for orchestrator processing.
        All search results go through here - no immediate manifestations.
        
        Args:
            manifestation_data: {
                "index": int, "collection": str, "score": float,
                "path": str, "description": str, "start": float, "end": float,
                "parameters": str, "sound_id": str, "freq_low": float, 
                "freq_high": float, "duration": float
            }
            
        Returns:
            True if queued successfully
        """
        try:
            request_time = time.time()
            self.queue.append((manifestation_data, request_time))
            
            sound_id = manifestation_data.get("sound_id", "unknown")
            freq_low = manifestation_data.get("freq_low", 200)
            freq_high = manifestation_data.get("freq_high", 2000)
            
            logger.debug(f"Queued manifestation: {sound_id} [{freq_low:.0f}-{freq_high:.0f}Hz]")
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue manifestation: {e}")
            return False
    
    def update(self):
        """
        Periodic update: clean expired niches and process queue.
        This is where manifestations actually get sent.
        """
        try:
            # Clean up expired niches
            self._cleanup_expired()
            
            # Process queue - try to manifest waiting sounds
            self._process_queue()
            
        except Exception as e:
            logger.error(f"Orchestrator update failed: {e}")
    
    def _process_queue(self):
        """Process the manifestation queue - send manifestations when niches are free."""
        if not self.queue or not self.manifest_callback:
            return
        
        now = time.time()
        remaining_queue = []
        manifestations_sent = 0
        
        # Process queue in FIFO order
        for manifestation_data, request_time in self.queue:
            try:
                # Extract frequency/duration info
                sound_id = manifestation_data.get("sound_id", "unknown")
                freq_low = float(manifestation_data.get("freq_low", 200))
                freq_high = float(manifestation_data.get("freq_high", 2000))
                duration = float(manifestation_data.get("duration", 1.0))
                
                # Check for conflicts
                conflict_end_time = self._find_conflict(freq_low, freq_high, now)
                
                if conflict_end_time is None:
                    # No conflict - register niche and send manifestation
                    self._register_niche(sound_id, now, now + duration, freq_low, freq_high)
                    
                    # Send manifestation via callback
                    self.manifest_callback(
                        manifestation_data["index"],
                        manifestation_data["collection"],
                        manifestation_data["score"],
                        manifestation_data["path"],
                        manifestation_data["description"],
                        manifestation_data["start"],
                        manifestation_data["end"],
                        manifestation_data["parameters"]
                    )
                    
                    manifestations_sent += 1
                    logger.debug(f"Manifested: {sound_id} [{freq_low:.0f}-{freq_high:.0f}Hz] "
                               f"(queued for {now - request_time:.1f}s)")
                else:
                    # Still has conflict - keep in queue
                    remaining_queue.append((manifestation_data, request_time))
                    
            except Exception as e:
                logger.error(f"Failed to process queued manifestation: {e}")
                # Drop this manifestation to avoid infinite loops
        
        # Update queue with remaining items
        self.queue = remaining_queue
        
        if manifestations_sent > 0:
            logger.debug(f"Processed queue: {manifestations_sent} manifestations sent, "
                        f"{len(self.queue)} still queued")
    
    def _find_conflict(self, freq_low: float, freq_high: float, now: float) -> Optional[float]:
        """
        Find if frequency range conflicts with active niches.
        
        Returns:
            None if no conflict, otherwise the end_time of the earliest conflicting niche
        """
        earliest_conflict_end = None
        
        for niche in self.active_niches:
            # Check time overlap (sound is still active)
            if now < niche["end_time"]:
                # Check logarithmic frequency overlap
                if self._has_frequency_overlap(freq_low, freq_high, 
                                             niche["freq_low"], niche["freq_high"]):
                    if earliest_conflict_end is None or niche["end_time"] < earliest_conflict_end:
                        earliest_conflict_end = niche["end_time"]
        
        return earliest_conflict_end
    
    def _has_frequency_overlap(self, f1_low: float, f1_high: float, 
                              f2_low: float, f2_high: float) -> bool:
        """
        Check if two frequency ranges overlap beyond threshold (logarithmic).
        
        Uses logarithmic frequency space to better match human perception.
        """
        try:
            # Convert to log space (base 2, so octaves)
            log_f1_low = math.log2(max(f1_low, 1))
            log_f1_high = math.log2(max(f1_high, 1))
            log_f2_low = math.log2(max(f2_low, 1))
            log_f2_high = math.log2(max(f2_high, 1))
            
            # Calculate ranges in log space
            range1_size = log_f1_high - log_f1_low
            range2_size = log_f2_high - log_f2_low
            
            # Find overlap region
            overlap_start = max(log_f1_low, log_f2_low)
            overlap_end = min(log_f1_high, log_f2_high)
            
            if overlap_start >= overlap_end:
                return False  # No overlap
            
            overlap_size = overlap_end - overlap_start
            
            # Check if overlap exceeds threshold relative to smaller range
            smaller_range = min(range1_size, range2_size)
            if smaller_range <= 0:
                return False
            
            overlap_ratio = overlap_size / smaller_range
            
            return overlap_ratio > self.overlap_threshold
            
        except (ValueError, ZeroDivisionError):
            # Handle edge cases
            return False
    
    def _register_niche(self, sound_id: str, start_time: float, end_time: float,
                       freq_low: float, freq_high: float):
        """Register a new active niche."""
        niche = {
            "sound_id": sound_id,
            "start_time": start_time,
            "end_time": end_time,
            "freq_low": freq_low,
            "freq_high": freq_high
        }
        self.active_niches.append(niche)
    
    def _cleanup_expired(self):
        """Remove expired niches."""
        now = time.time()
        
        before_count = len(self.active_niches)
        self.active_niches = [n for n in self.active_niches if n["end_time"] > now]
        
        if len(self.active_niches) < before_count:
            logger.debug(f"Cleaned up {before_count - len(self.active_niches)} expired niches")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        return {
            "active_niches": len(self.active_niches),
            "queued_requests": len(self.queue),
            "overlap_threshold": self.overlap_threshold,
            "time_precision": self.time_precision
        }
    
    # Legacy method for backward compatibility (no longer used)
    def evaluate_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy method - now everything goes through queue."""
        logger.warning("evaluate_request() called - should use queue_manifestation() instead")
        return {"status": "allowed", "start_time": time.time()}