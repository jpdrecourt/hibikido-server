"""
Database Manager for Hibikidō (Updated for Path-based Schema)
============================================================

Handles MongoDB operations for the hierarchical schema with path-based references:
- recordings: Source audio files (indexed by path)
- segments: Timestamped slices of recordings (reference by source_path)
- effects: Audio processing tools (indexed by path)  
- presets: Effect configurations (reference by effect_path, separate collection)
- performances: Session logs
- segmentations: Batch processing metadata
"""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging

logger = logging.getLogger(__name__)

class HibikidoDatabase:
    def __init__(self, uri: str = "mongodb://localhost:27017", 
                 db_name: str = "hibikido"):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        
        # Collection references
        self.recordings = None
        self.segments = None
        self.effects = None
        self.presets = None  # Now separate collection
        self.performances = None
        self.segmentations = None
    
    def connect(self) -> bool:
        """Initialize MongoDB connection and setup collections."""
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            
            # Initialize collections
            self.recordings = self.db.recordings
            self.segments = self.db.segments
            self.effects = self.db.effects
            self.presets = self.db.presets  # New separate collection
            self.performances = self.db.performances
            self.segmentations = self.db.segmentations
            
            # Create indexes for better performance
            self._create_indexes()
            
            logger.info(f"Hibikidō database connected: {self.db_name}")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    def _create_indexes(self):
        """Create database indexes for optimal performance."""
        try:
            # Recordings indexes (path-based)
            self.recordings.create_index("path", unique=True)
            self.recordings.create_index([("description", "text")])
            
            # Segments indexes (reference by source_path)
            self.segments.create_index("source_path")
            self.segments.create_index("segmentation_id")
            self.segments.create_index("FAISS_index", unique=True, sparse=True)
            self.segments.create_index([("description", "text"), ("embedding_text", "text")])
            self.segments.create_index([("start", 1), ("end", 1)])
            
            # Effects indexes (path-based)
            self.effects.create_index("path", unique=True)
            self.effects.create_index("name")
            self.effects.create_index([("description", "text")])
            
            # Presets indexes (separate collection, reference by effect_path)
            self.presets.create_index("effect_path")
            self.presets.create_index("FAISS_index", unique=True, sparse=True)
            self.presets.create_index([("description", "text"), ("embedding_text", "text")])
            
            # Performances indexes
            self.performances.create_index("date")
            self.performances.create_index("invocations.segment_id")
            self.performances.create_index("invocations.effect")
            
            # Segmentations indexes
            self.segmentations.create_index("method")
            
            logger.debug("Database indexes created")
        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    # RECORDINGS METHODS (path-based)
    
    def add_recording(self, path: str, description: str) -> bool:
        """Add a new recording using path as unique identifier."""
        try:
            recording = {
                "path": path,
                "description": description,
                "created_at": datetime.now()
            }
            
            self.recordings.insert_one(recording)
            logger.info(f"Added recording: {path}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate recording path: {path}")
            return False
        except Exception as e:
            logger.error(f"Failed to add recording: {e}")
            return False
    
    def get_recording_by_path(self, path: str) -> Optional[Dict[str, Any]]:
        """Get recording by path."""
        try:
            return self.recordings.find_one({"path": path})
        except Exception as e:
            logger.error(f"Failed to get recording {path}: {e}")
            return None
    
    def get_all_recordings(self) -> List[Dict[str, Any]]:
        """Get all recordings."""
        try:
            return list(self.recordings.find())
        except Exception as e:
            logger.error(f"Failed to get recordings: {e}")
            return []
    
    # SEGMENTS METHODS (reference by source_path)
    
    def add_segment(self, source_path: str, segmentation_id: str,
                   start: float, end: float, description: str, 
                   embedding_text: str, faiss_index: int = None) -> bool:
        """Add a new segment referencing recording by path."""
        try:
            segment = {
                "source_path": source_path,
                "segmentation_id": segmentation_id,
                "start": start,
                "end": end,
                "description": description,
                "embedding_text": embedding_text,
                "created_at": datetime.now()
            }
            
            if faiss_index is not None:
                segment["FAISS_index"] = faiss_index
            
            result = self.segments.insert_one(segment)
            logger.info(f"Added segment: {result.inserted_id} - {description[:50]}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add segment: {e}")
            return False
    
    def get_segment_by_faiss_id(self, faiss_index: int) -> Optional[Dict[str, Any]]:
        """Get segment by FAISS index."""
        try:
            return self.segments.find_one({"FAISS_index": faiss_index})
        except Exception as e:
            logger.error(f"Failed to get segment with FAISS index {faiss_index}: {e}")
            return None
    
    def get_segments_by_recording_path(self, source_path: str) -> List[Dict[str, Any]]:
        """Get all segments for a recording by path."""
        try:
            return list(self.segments.find({"source_path": source_path}).sort("start", 1))
        except Exception as e:
            logger.error(f"Failed to get segments for recording {source_path}: {e}")
            return []
        
    def add_segmentation(self, segmentation_id: str, method: str, 
                    parameters: Dict[str, Any] = None, 
                    description: str = "") -> bool:
        """Add a new segmentation method/run."""
        try:
            segmentation = {
                "_id": segmentation_id,
                "method": method,
                "parameters": parameters or {},
                "description": description,
                "created_at": datetime.now()
            }
            
            self.segmentations.insert_one(segmentation)
            logger.info(f"Added segmentation: {segmentation_id} - {method}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate segmentation ID: {segmentation_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to add segmentation: {e}")
            return False

    def get_segmentation(self, segmentation_id: str) -> Optional[Dict[str, Any]]:
        """Get segmentation by ID."""
        try:
            return self.segmentations.find_one({"_id": segmentation_id})
        except Exception as e:
            logger.error(f"Failed to get segmentation {segmentation_id}: {e}")
            return None
        
    # EFFECTS METHODS (path-based)
    
    def add_effect(self, path: str, name: str, description: str = "") -> bool:
        """Add a new effect using path as unique identifier."""
        try:
            effect = {
                "path": path,
                "name": name,
                "description": description,
                "created_at": datetime.now()
            }
            
            self.effects.insert_one(effect)
            logger.info(f"Added effect: {path} - {name}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate effect path: {path}")
            return False
        except Exception as e:
            logger.error(f"Failed to add effect: {e}")
            return False
    
    def get_effect_by_path(self, path: str) -> Optional[Dict[str, Any]]:
        """Get effect by path."""
        try:
            return self.effects.find_one({"path": path})
        except Exception as e:
            logger.error(f"Failed to get effect {path}: {e}")
            return None
    
    # PRESETS METHODS (separate collection, reference by effect_path)
    
    def add_preset(self, effect_path: str, parameters: List[Any], 
                  description: str, embedding_text: str, 
                  faiss_index: int = None) -> bool:
        """Add a new preset to separate presets collection."""
        try:
            preset = {
                "effect_path": effect_path,
                "parameters": parameters,
                "description": description,
                "embedding_text": embedding_text,
                "created_at": datetime.now()
            }
            
            if faiss_index is not None:
                preset["FAISS_index"] = faiss_index
            
            result = self.presets.insert_one(preset)
            logger.info(f"Added preset: {result.inserted_id} - {description[:50]}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add preset: {e}")
            return False
    
    def get_preset_by_faiss_id(self, faiss_index: int) -> Optional[Dict[str, Any]]:
        """Get preset by FAISS index."""
        try:
            return self.presets.find_one({"FAISS_index": faiss_index})
        except Exception as e:
            logger.error(f"Failed to get preset with FAISS index {faiss_index}: {e}")
            return None
    
    def get_presets_by_effect_path(self, effect_path: str) -> List[Dict[str, Any]]:
        """Get all presets for an effect by path."""
        try:
            return list(self.presets.find({"effect_path": effect_path}))
        except Exception as e:
            logger.error(f"Failed to get presets for effect {effect_path}: {e}")
            return []
    
    def get_segments_without_embeddings(self) -> List[Dict[str, Any]]:
        """Get all segments that don't have FAISS embeddings yet."""
        try:
            return list(self.segments.find({"FAISS_index": {"$exists": False}}))
        except Exception as e:
            logger.error(f"Failed to get segments without embeddings: {e}")
            return []
    
    def get_presets_without_embeddings(self) -> List[Dict[str, Any]]:
        """Get all presets that don't have FAISS embeddings yet."""
        try:
            return list(self.presets.find({"FAISS_index": {"$exists": False}}))
        except Exception as e:
            logger.error(f"Failed to get presets without embeddings: {e}")
            return []
    
    # PERFORMANCES METHODS (unchanged)
    
    def add_performance(self, performance_id: str, date: datetime = None) -> bool:
        """Add a new performance session."""
        try:
            performance = {
                "_id": performance_id,
                "date": date or datetime.now(),
                "invocations": [],
                "created_at": datetime.now()
            }
            
            self.performances.insert_one(performance)
            logger.info(f"Added performance: {performance_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate performance ID: {performance_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to add performance: {e}")
            return False
    
    def add_invocation(self, performance_id: str, text: str, time: float,
                      segment_id: str = None, effect: str = None) -> bool:
        """Add an invocation to a performance."""
        try:
            invocation = {
                "text": text,
                "time": time
            }
            
            if segment_id:
                invocation["segment_id"] = segment_id
            if effect:
                invocation["effect"] = effect
            
            result = self.performances.update_one(
                {"_id": performance_id},
                {"$push": {"invocations": invocation}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to add invocation to performance {performance_id}: {e}")
            return False
    
    # STATISTICS AND UTILITIES
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        try:
            recordings_count = self.recordings.count_documents({})
            segments_count = self.segments.count_documents({})
            segments_with_embeddings = self.segments.count_documents({"FAISS_index": {"$exists": True}})
            effects_count = self.effects.count_documents({})
            presets_count = self.presets.count_documents({})  # Now separate collection
            presets_with_embeddings = self.presets.count_documents({"FAISS_index": {"$exists": True}})
            performances_count = self.performances.count_documents({})
            segmentations_count = self.segmentations.count_documents({})
            
            return {
                "recordings": recordings_count,
                "segments": segments_count,
                "segments_with_embeddings": segments_with_embeddings,
                "effects": effects_count,
                "presets": presets_count,
                "presets_with_embeddings": presets_with_embeddings,
                "performances": performances_count,
                "segmentations": segmentations_count,
                "total_searchable_items": segments_with_embeddings + presets_with_embeddings
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    def close(self):
        """Close the database connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")