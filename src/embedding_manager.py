"""
Embedding Manager for Incantation Server
========================================

Handles sentence transformer models and FAISS operations.
"""

import os
import numpy as np
from typing import List, Tuple, Optional
import faiss
import torch
from sentence_transformers import SentenceTransformer
import logging

logger = logging.getLogger(__name__)

class EmbeddingManager:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", 
                 index_file: str = "incantations.index"):
        self.model_name = model_name
        self.index_file = index_file
        self.model = None
        self.index = None
        self.next_id = 0
        self.embedding_dim = 384  # Dimension for all-MiniLM-L6-v2
    
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
            
            try:
                self.model = SentenceTransformer(self.model_name, device=device, local_files_only=True)
                logger.info(f"Embedding model loaded on: {device.upper()}")
            except Exception as e:
                logger.warning(f"Failed to load model on GPU, falling back to CPU: {e}")
                device = "cpu"
                self.model = SentenceTransformer(self.model_name, device=device)
                logger.info("Embedding model loaded on: CPU")
            
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
            
            logger.info(f"Next FAISS ID will be: {self.next_id}")
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
    
    def encode_text(self, text: str) -> np.ndarray:
        """Encode text into embedding vector."""
        try:
            return self.model.encode(text.lower().strip(), normalize_embeddings=True)
        except Exception as e:
            logger.error(f"Failed to encode text: {e}")
            return None
    
    def add_embedding(self, text: str) -> Tuple[Optional[int], bool]:
        """
        Add text embedding to FAISS index.
        Returns (faiss_id, is_duplicate)
        """
        try:
            embedding = self.encode_text(text)
            if embedding is None:
                return None, False
            
            embedding = embedding.reshape(1, -1)
            
            # Check for duplicates
            if self.index.ntotal > 0:
                scores, indices = self.index.search(embedding, min(5, self.index.ntotal))
                for i, score in enumerate(scores[0]):
                    if abs(score - 1.0) < 1e-6:  # Very close to 1.0 (exact match)
                        duplicate_id = int(indices[0][i])
                        logger.warning(f"Duplicate embedding detected: FAISS ID {duplicate_id}")
                        return duplicate_id, True
            
            # Add to index
            faiss_id = self.next_id
            self.index.add(embedding)
            self.next_id += 1
            
            self._save_index()
            logger.debug(f"Added embedding with FAISS ID {faiss_id}")
            return faiss_id, False
            
        except Exception as e:
            logger.error(f"Failed to add embedding: {e}")
            return None, False
    
    def search(self, query_text: str, top_k: int = 10) -> Tuple[List[int], List[float]]:
        """
        Search for similar embeddings.
        Returns (faiss_ids, scores)
        """
        try:
            if self.index.ntotal == 0:
                return [], []
            
            query_embedding = self.encode_text(query_text)
            if query_embedding is None:
                return [], []
            
            query_embedding = query_embedding.reshape(1, -1)
            
            k = min(top_k, self.index.ntotal)
            scores, indices = self.index.search(query_embedding, k)
            
            faiss_ids = [int(idx) for idx in indices[0]]
            similarity_scores = [float(score) for score in scores[0]]
            
            return faiss_ids, similarity_scores
            
        except Exception as e:
            logger.error(f"Failed to search embeddings: {e}")
            return [], []
    
    def get_total_embeddings(self) -> int:
        """Get total number of embeddings in the index."""
        return self.index.ntotal if self.index else 0