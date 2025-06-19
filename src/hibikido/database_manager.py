"""
Database Manager for Hibikidō
=============================

Handles MongoDB operations for the hierarchical schema:
- recordings: Source audio files
- segments: Timestamped slices of recordings
- effects: Audio processing with presets
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
            # Recordings indexes
            self.recordings.create_index("path", unique=True)
            self.recordings.create_index([("description", "text")])
            
            # Segments indexes  
            self.segments.create_index("source_id")
            self.segments.create_index("segmentation_id")
            self.segments.create_index("FAISS_index", unique=True, sparse=True)
            self.segments.create_index([("description", "text"), ("embedding_text", "text")])
            self.segments.create_index([("start", 1), ("end", 1)])
            
            # Effects indexes
            self.effects.create_index("name")
            self.effects.create_index("path")
            self.effects.create_index("presets.FAISS_index", sparse=True)
            self.effects.create_index([("description", "text"), ("presets.description", "text")])
            
            # Performances indexes
            self.performances.create_index("date")
            self.performances.create_index("invocations.segment_id")
            self.performances.create_index("invocations.effect")
            
            # Segmentations indexes
            self.segmentations.create_index("method")
            
            logger.debug("Database indexes created")
        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    # RECORDINGS METHODS
    
    def add_recording(self, recording_id: str, path: str, description: str) -> bool:
        """Add a new recording."""
        try:
            recording = {
                "_id": recording_id,
                "path": path,
                "description": description,
                "created_at": datetime.now()
            }
            
            self.recordings.insert_one(recording)
            logger.info(f"Added recording: {recording_id} - {path}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate recording ID: {recording_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to add recording: {e}")
            return False
    
    def get_recording(self, recording_id: str) -> Optional[Dict[str, Any]]:
        """Get recording by ID."""
        try:
            return self.recordings.find_one({"_id": recording_id})
        except Exception as e:
            logger.error(f"Failed to get recording {recording_id}: {e}")
            return None
    
    def get_all_recordings(self) -> List[Dict[str, Any]]:
        """Get all recordings."""
        try:
            return list(self.recordings.find())
        except Exception as e:
            logger.error(f"Failed to get recordings: {e}")
            return []
    
    def update_recording(self, recording_id: str, updates: Dict[str, Any]) -> bool:
        """Update recording metadata."""
        try:
            updates["updated_at"] = datetime.now()
            result = self.recordings.update_one(
                {"_id": recording_id},
                {"$set": updates}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update recording {recording_id}: {e}")
            return False
    
    # SEGMENTATIONS METHODS
    
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
    
    # SEGMENTS METHODS
    
    def add_segment(self, segment_id: str, source_id: str, segmentation_id: str,
                   start: float, end: float, description: str, 
                   embedding_text: str, faiss_index: int = None) -> bool:
        """Add a new segment."""
        try:
            segment = {
                "_id": segment_id,
                "source_id": source_id,
                "segmentation_id": segmentation_id,
                "start": start,
                "end": end,
                "description": description,
                "embedding_text": embedding_text,
                "created_at": datetime.now()
            }
            
            if faiss_index is not None:
                segment["FAISS_index"] = faiss_index
            
            self.segments.insert_one(segment)
            logger.info(f"Added segment: {segment_id} - {description[:50]}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate segment ID: {segment_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to add segment: {e}")
            return False
    
    def get_segment(self, segment_id: str) -> Optional[Dict[str, Any]]:
        """Get segment by ID."""
        try:
            return self.segments.find_one({"_id": segment_id})
        except Exception as e:
            logger.error(f"Failed to get segment {segment_id}: {e}")
            return None
    
    def get_segment_by_faiss_id(self, faiss_index: int) -> Optional[Dict[str, Any]]:
        """Get segment by FAISS index."""
        try:
            return self.segments.find_one({"FAISS_index": faiss_index})
        except Exception as e:
            logger.error(f"Failed to get segment with FAISS index {faiss_index}: {e}")
            return None
    
    def get_segments_by_recording(self, source_id: str) -> List[Dict[str, Any]]:
        """Get all segments for a recording."""
        try:
            return list(self.segments.find({"source_id": source_id}).sort("start", 1))
        except Exception as e:
            logger.error(f"Failed to get segments for recording {source_id}: {e}")
            return []
    
    def get_segments_by_segmentation(self, segmentation_id: str) -> List[Dict[str, Any]]:
        """Get all segments from a segmentation run."""
        try:
            return list(self.segments.find({"segmentation_id": segmentation_id}))
        except Exception as e:
            logger.error(f"Failed to get segments for segmentation {segmentation_id}: {e}")
            return []
    
    def update_segment_faiss_index(self, segment_id: str, faiss_index: int) -> bool:
        """Update the FAISS index for a segment."""
        try:
            result = self.segments.update_one(
                {"_id": segment_id},
                {"$set": {"FAISS_index": faiss_index, "updated_at": datetime.now()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update FAISS index for segment {segment_id}: {e}")
            return False
    
    def get_segments_without_embeddings(self) -> List[Dict[str, Any]]:
        """Get all segments that don't have FAISS embeddings yet."""
        try:
            return list(self.segments.find({"FAISS_index": {"$exists": False}}))
        except Exception as e:
            logger.error(f"Failed to get segments without embeddings: {e}")
            return []
    
    # EFFECTS METHODS
    
    def add_effect(self, effect_id: str, name: str, path: str, 
                  description: str = "", presets: List[Dict[str, Any]] = None) -> bool:
        """Add a new effect."""
        try:
            effect = {
                "_id": effect_id,
                "name": name,
                "path": path,
                "description": description,
                "presets": presets or [],
                "created_at": datetime.now()
            }
            
            self.effects.insert_one(effect)
            logger.info(f"Added effect: {effect_id} - {name}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate effect ID: {effect_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to add effect: {e}")
            return False
    
    def get_effect(self, effect_id: str) -> Optional[Dict[str, Any]]:
        """Get effect by ID."""
        try:
            return self.effects.find_one({"_id": effect_id})
        except Exception as e:
            logger.error(f"Failed to get effect {effect_id}: {e}")
            return None
    
    def add_preset_to_effect(self, effect_id: str, preset: Dict[str, Any]) -> bool:
        """Add a preset to an existing effect."""
        try:
            # Validate preset structure
            required_fields = ["parameters", "description", "embedding_text"]
            if not all(field in preset for field in required_fields):
                logger.error(f"Preset missing required fields: {required_fields}")
                return False
            
            result = self.effects.update_one(
                {"_id": effect_id},
                {"$push": {"presets": preset}, "$set": {"updated_at": datetime.now()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to add preset to effect {effect_id}: {e}")
            return False
    
    def get_preset_by_faiss_id(self, faiss_index: int) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """Get effect and preset by FAISS index."""
        try:
            effect = self.effects.find_one({"presets.FAISS_index": faiss_index})
            if effect:
                for preset in effect.get("presets", []):
                    if preset.get("FAISS_index") == faiss_index:
                        return effect, preset
            return None
        except Exception as e:
            logger.error(f"Failed to get preset with FAISS index {faiss_index}: {e}")
            return None
    
    def update_preset_faiss_index(self, effect_id: str, preset_index: int, faiss_index: int) -> bool:
        """Update the FAISS index for a specific preset."""
        try:
            result = self.effects.update_one(
                {"_id": effect_id},
                {"$set": {
                    f"presets.{preset_index}.FAISS_index": faiss_index,
                    "updated_at": datetime.now()
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update preset FAISS index: {e}")
            return False
    
    def get_presets_without_embeddings(self) -> List[Tuple[str, int, Dict[str, Any]]]:
        """Get all presets that don't have FAISS embeddings yet."""
        try:
            results = []
            effects = self.effects.find({"presets": {"$exists": True}})
            
            for effect in effects:
                for i, preset in enumerate(effect.get("presets", [])):
                    if "FAISS_index" not in preset:
                        results.append((effect["_id"], i, preset))
            
            return results
        except Exception as e:
            logger.error(f"Failed to get presets without embeddings: {e}")
            return []
    
    # PERFORMANCES METHODS
    
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
    
    def get_performance(self, performance_id: str) -> Optional[Dict[str, Any]]:
        """Get performance by ID."""
        try:
            return self.performances.find_one({"_id": performance_id})
        except Exception as e:
            logger.error(f"Failed to get performance {performance_id}: {e}")
            return None
    
    # STATISTICS AND UTILITIES
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        try:
            recordings_count = self.recordings.count_documents({})
            segments_count = self.segments.count_documents({})
            segments_with_embeddings = self.segments.count_documents({"FAISS_index": {"$exists": True}})
            effects_count = self.effects.count_documents({})
            performances_count = self.performances.count_documents({})
            segmentations_count = self.segmentations.count_documents({})
            
            # Get preset count
            pipeline = [
                {"$project": {"preset_count": {"$size": {"$ifNull": ["$presets", []]}}}},
                {"$group": {"_id": None, "total_presets": {"$sum": "$preset_count"}}}
            ]
            preset_result = list(self.effects.aggregate(pipeline))
            presets_count = preset_result[0]["total_presets"] if preset_result else 0
            
            # Get presets with embeddings
            presets_with_embeddings = 0
            for effect in self.effects.find({"presets": {"$exists": True}}):
                for preset in effect.get("presets", []):
                    if "FAISS_index" in preset:
                        presets_with_embeddings += 1
            
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