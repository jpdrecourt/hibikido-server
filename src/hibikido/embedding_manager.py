"""
Embedding Manager for Hibikidō
==============================

Simple FAISS search with MongoDB document retrieval.
FAISS indices stored in MongoDB documents as per original schema.
"""

import os
import faiss
import torch
from sentence_transformers import SentenceTransformer
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class EmbeddingManager:
    """Simple embedding manager following original database design."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", 
                 index_file: str = "hibikido.index"):
        self.model_name = model_name
        self.index_file = index_file
        self.model = None
        self.index = None
        self.next_id = 0
        self.embedding_dim = 384  # all-MiniLM-L6-v2 dimension
    
    def initialize(self) -> bool:
        """Initialize the embedding model and FAISS index."""
        if not self._load_model():
            return False
        if not self._load_or_create_index():
            return False
        return True
    
    def _load_model(self) -> bool:
        """Load the sentence transformer model."""
        try:
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Loading embedding model: {self.model_name}")
            
            self.model = SentenceTransformer(self.model_name, device=device)
            logger.info(f"Embedding model loaded on: {device.upper()}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            return False
    
    def _load_or_create_index(self) -> bool:
        """Load existing FAISS index or create a new one."""
        try:
            if os.path.exists(self.index_file):
                self.index = faiss.read_index(self.index_file)
                self.next_id = self.index.ntotal
                logger.info(f"Loaded FAISS index with {self.index.ntotal} entries")
            else:
                self.index = faiss.IndexFlatIP(self.embedding_dim)
                self.next_id = 0
                self._save_index()
                logger.info("Created new FAISS index")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize FAISS index: {e}")
            return False
    
    def _save_index(self) -> bool:
        """Save FAISS index to disk."""
        try:
            faiss.write_index(self.index, self.index_file)
            logger.debug("FAISS index saved")
            return True
        except Exception as e:
            logger.error(f"Failed to save FAISS index: {e}")
            return False
    
    def add_embedding(self, text: str) -> Optional[int]:
        """
        Add text embedding to FAISS index.
        
        Args:
            text: Text to embed
            
        Returns:
            FAISS index ID or None if failed
        """
        try:
            if not text or not text.strip():
                logger.warning("Empty text provided for embedding")
                return None
            
            # Create embedding
            embedding = self.model.encode(text.strip(), normalize_embeddings=True)
            embedding = embedding.reshape(1, -1)
            
            # Add to FAISS index
            faiss_id = self.next_id
            self.index.add(embedding)
            self.next_id += 1
            
            # Save to disk
            self._save_index()
            
            logger.debug(f"Added embedding {faiss_id}")
            return faiss_id
            
        except Exception as e:
            logger.error(f"Failed to add embedding: {e}")
            return None
    
    def search(self, query: str, top_k: int = 10, db_manager=None) -> List[Dict[str, Any]]:
        """
        Search FAISS index and return MongoDB documents.
        
        Args:
            query: Search query text
            top_k: Maximum number of results
            db_manager: Database manager for MongoDB lookups
            
        Returns:
            List of {"collection": str, "document": dict, "score": float} dicts
        """
        try:
            if not query or not query.strip():
                return []
            
            if self.index.ntotal == 0:
                logger.info("Search called on empty index")
                return []
            
            if not db_manager:
                logger.error("Database manager required for search")
                return []
            
            # Create query embedding
            query_embedding = self.model.encode(query.strip(), normalize_embeddings=True)
            query_embedding = query_embedding.reshape(1, -1)
            
            # Search FAISS
            k = min(top_k, self.index.ntotal)
            scores, indices = self.index.search(query_embedding, k)
            
            # MongoDB lookups by FAISS_index
            results = []
            for faiss_idx, score in zip(indices[0], scores[0]):
                # Convert numpy.int64 to regular Python int for MongoDB
                faiss_idx = int(faiss_idx)
                
                # Look for segment with this FAISS_index
                segment = db_manager.segments.find_one({"FAISS_index": faiss_idx})
                if segment:
                    results.append({
                        "collection": "segments",
                        "document": segment,
                        "score": float(score)
                    })
                    continue
                
                # Look for preset with this FAISS_index
                effect = db_manager.effects.find_one({"presets.FAISS_index": faiss_idx})
                if effect:
                    preset = next((p for p in effect.get("presets", []) 
                                 if p.get("FAISS_index") == faiss_idx), None)
                    if preset:
                        results.append({
                            "collection": "presets", 
                            "document": preset,
                            "effect": effect,
                            "score": float(score)
                        })
            
            logger.info(f"Search '{query}' returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Search failed for '{query}': {e}")
            return []
    
    def get_total_embeddings(self) -> int:
        """Get total number of embeddings."""
        return self.index.ntotal if self.index else 0
    
    def rebuild_from_database(self, db_manager, text_processor=None) -> Dict[str, int]:
        """
        Rebuild entire FAISS index from MongoDB with hierarchical context.
        
        Args:
            db_manager: HibikidoDatabase instance
            text_processor: TextProcessor for hierarchical embedding text
            
        Returns:
            Statistics about rebuild process
        """
        logger.info("Rebuilding FAISS index from database with hierarchical context...")
        
        stats = {
            "segments_processed": 0,
            "segments_added": 0,
            "presets_processed": 0,
            "presets_added": 0,
            "errors": 0
        }
        
        try:
            # Reset index
            self.index = faiss.IndexFlatIP(self.embedding_dim)
            self.next_id = 0
            
            # Process segments with hierarchical context
            if text_processor:
                segments = db_manager.segments.find({})
                for segment in segments:
                    try:
                        stats["segments_processed"] += 1
                        
                        # Get context for hierarchical embedding
                        recording = db_manager.get_recording(segment.get("source_id"))
                        segmentation = db_manager.get_segmentation(segment.get("segmentation_id"))
                        
                        # Create hierarchical embedding text
                        embedding_text = text_processor.create_segment_embedding_text(
                            segment, recording, segmentation
                        )
                        
                        if embedding_text:
                            faiss_id = self.add_embedding(embedding_text)
                            
                            if faiss_id is not None:
                                # Update segment with new FAISS_index and embedding_text
                                db_manager.segments.update_one(
                                    {"_id": segment["_id"]},
                                    {"$set": {
                                        "FAISS_index": faiss_id,
                                        "embedding_text": embedding_text
                                    }}
                                )
                                stats["segments_added"] += 1
                    
                    except Exception as e:
                        stats["errors"] += 1
                        logger.error(f"Failed to process segment {segment.get('_id', 'unknown')}: {e}")
            
            else:
                # Fallback to existing embedding_text
                segments = db_manager.segments.find({})
                for segment in segments:
                    try:
                        stats["segments_processed"] += 1
                        
                        embedding_text = segment.get("embedding_text", "")
                        if embedding_text:
                            faiss_id = self.add_embedding(embedding_text)
                            
                            if faiss_id is not None:
                                db_manager.segments.update_one(
                                    {"_id": segment["_id"]},
                                    {"$set": {"FAISS_index": faiss_id}}
                                )
                                stats["segments_added"] += 1
                    
                    except Exception as e:
                        stats["errors"] += 1
                        logger.error(f"Failed to process segment {segment.get('_id', 'unknown')}: {e}")
            
            # Process effect presets with hierarchical context
            effects = db_manager.effects.find({"presets": {"$exists": True}})
            for effect in effects:
                try:
                    presets = effect.get("presets", [])
                    for preset_idx, preset in enumerate(presets):
                        stats["presets_processed"] += 1
                        
                        if text_processor:
                            # Create hierarchical embedding text
                            embedding_text = text_processor.create_preset_embedding_text(preset, effect)
                        else:
                            # Fallback to existing embedding_text
                            embedding_text = preset.get("embedding_text", "")
                        
                        if embedding_text:
                            faiss_id = self.add_embedding(embedding_text)
                            
                            if faiss_id is not None:
                                # Update preset with new FAISS_index and embedding_text
                                update_data = {f"presets.{preset_idx}.FAISS_index": faiss_id}
                                if text_processor:
                                    update_data[f"presets.{preset_idx}.embedding_text"] = embedding_text
                                
                                db_manager.effects.update_one(
                                    {"_id": effect["_id"]},
                                    {"$set": update_data}
                                )
                                stats["presets_added"] += 1
                
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Failed to process effect {effect.get('_id', 'unknown')}: {e}")
            
            logger.info(f"Index rebuild complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Index rebuild failed: {e}")
            stats["errors"] += 1
            return stats