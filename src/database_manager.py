"""
Database Manager for Incantation Server
======================================

Handles all MongoDB operations and database interactions.
"""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, uri: str = "mongodb://localhost:27017", 
                 db_name: str = "incantations", 
                 collection_name: str = "entries"):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self) -> bool:
        """Initialize MongoDB connection and setup collection."""
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            # Create indexes for better performance
            self._create_indexes()
            
            logger.info(f"MongoDB connected: {self.db_name}.{self.collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
    
    def _create_indexes(self):
        """Create database indexes for optimal performance."""
        try:
            self.collection.create_index("faiss_id", unique=True, sparse=True)
            self.collection.create_index("ID", unique=True, sparse=True)
            self.collection.create_index("type")
            self.collection.create_index("deleted")
            self.collection.create_index([("title", "text"), ("description", "text")])
            logger.debug("Database indexes created")
        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    def get_next_id(self) -> int:
        """Get the next available ID for new entries."""
        try:
            result = self.collection.find_one(sort=[("ID", -1)])
            if result and "ID" in result:
                return result["ID"] + 1
            return 1
        except Exception:
            return 1
    
    def add_entry(self, entry: Dict[str, Any]) -> bool:
        """Add a new entry to the database."""
        try:
            # Ensure required fields
            entry.setdefault("created_at", datetime.now())
            entry.setdefault("deleted", False)
            
            self.collection.insert_one(entry)
            logger.info(f"Added entry ID {entry.get('ID')} - {entry.get('title', 'untitled')}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"Duplicate entry ID: {entry.get('ID')}")
            return False
        except Exception as e:
            logger.error(f"Failed to add entry: {e}")
            return False
    
    def get_by_id(self, entry_id: int) -> Optional[Dict[str, Any]]:
        """Get entry by ID."""
        try:
            return self.collection.find_one({
                "ID": entry_id, 
                "deleted": {"$ne": True}
            })
        except Exception as e:
            logger.error(f"Failed to get entry {entry_id}: {e}")
            return None
    
    def get_by_faiss_id(self, faiss_id: int) -> Optional[Dict[str, Any]]:
        """Get entry by FAISS ID."""
        try:
            return self.collection.find_one({
                "faiss_id": faiss_id,
                "deleted": {"$ne": True}
            })
        except Exception as e:
            logger.error(f"Failed to get entry with FAISS ID {faiss_id}: {e}")
            return None
    
    def soft_delete(self, entry_id: int) -> bool:
        """Soft delete an entry by marking it as deleted."""
        try:
            result = self.collection.update_one(
                {"ID": entry_id},
                {"$set": {"deleted": True, "deleted_at": datetime.now()}}
            )
            
            if result.modified_count > 0:
                logger.info(f"Soft deleted entry ID {entry_id}")
                return True
            else:
                logger.warning(f"Entry ID {entry_id} not found for deletion")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete entry {entry_id}: {e}")
            return False
    
    def update_embedding_info(self, entry_id: int, faiss_id: int, embedding_text: str) -> bool:
        """Update the FAISS ID and embedding text for an entry."""
        try:
            result = self.collection.update_one(
                {"ID": entry_id},
                {"$set": {
                    "faiss_id": faiss_id,
                    "embedding_text": embedding_text,
                    "updated_at": datetime.now()
                }}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated embedding for entry ID {entry_id} -> FAISS {faiss_id}")
                return True
            else:
                logger.warning(f"Entry ID {entry_id} not found for update")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update embedding for entry {entry_id}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            total = self.collection.count_documents({})
            active = self.collection.count_documents({"deleted": {"$ne": True}})
            deleted = self.collection.count_documents({"deleted": True})
            with_embeddings = self.collection.count_documents({
                "faiss_id": {"$exists": True},
                "deleted": {"$ne": True}
            })
            
            # Get type breakdown
            pipeline = [
                {"$match": {"deleted": {"$ne": True}}},
                {"$group": {"_id": "$type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            type_counts = list(self.collection.aggregate(pipeline))
            
            return {
                "total": total,
                "active": active,
                "deleted": deleted,
                "with_embeddings": with_embeddings,
                "type_breakdown": {item["_id"]: item["count"] for item in type_counts}
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    def get_types(self) -> List[str]:
        """Get all available entry types."""
        try:
            types = self.collection.distinct("type", {"deleted": {"$ne": True}})
            return [t for t in types if t]  # Remove empty types
        except Exception as e:
            logger.error(f"Failed to get types: {e}")
            return []
    
    def get_entries_without_embeddings(self) -> List[Dict[str, Any]]:
        """Get all entries that don't have FAISS embeddings yet."""
        try:
            return list(self.collection.find({
                "faiss_id": {"$exists": False},
                "deleted": {"$ne": True}
            }))
        except Exception as e:
            logger.error(f"Failed to get entries without embeddings: {e}")
            return []
    
    def close(self):
        """Close the database connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")