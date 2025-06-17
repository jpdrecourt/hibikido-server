"""
Text Processor for Incantation Server
====================================

Utilities for creating optimized embedding sentences from CSV data
and other text processing tasks.
"""

import re
from typing import Dict, Any, List, Optional
import logging
import os

logger = logging.getLogger(__name__)

class TextProcessor:
    def __init__(self):
        # Musical and audio descriptive terms that enhance semantic understanding
        self.audio_descriptors = {
            'percussion': ['drums', 'beats', 'rhythm', 'percussive'],
            'melodic': ['melody', 'melodic', 'harmonic', 'musical'],
            'ambient': ['atmosphere', 'ambient', 'spacious', 'ethereal'],
            'texture': ['smooth', 'rough', 'gritty', 'clean', 'distorted'],
            'dynamics': ['loud', 'quiet', 'soft', 'intense', 'gentle'],
            'temporal': ['fast', 'slow', 'quick', 'sustained', 'short'],
            'tonal': ['bright', 'dark', 'warm', 'cold', 'rich', 'thin']
        }
    
    def create_embedding_sentence(self, entry: Dict[str, Any]) -> str:
        """
        Create an optimized embedding sentence from entry data.
        Uses ONLY description and filename for semantic matching.
        """
        try:
            parts = []
            
            # Add description (primary content)
            description = self._get_field_value(entry, ['description', 'Description'])
            if description:
                cleaned_desc = self._process_description(description)
                if cleaned_desc:
                    parts.append(cleaned_desc)
            
            # Add filename for context (if relevant and no description)
            if not parts:  # Only use filename if no description
                filename = self._get_field_value(entry, ['file', 'File'])
                if filename:
                    cleaned_filename = self._process_filename(filename)
                    if cleaned_filename:
                        parts.append(cleaned_filename)
            
            # Combine and clean
            sentence = ' '.join(parts)
            sentence = self._normalize_sentence(sentence)
            
            logger.debug(f"Created embedding sentence: '{sentence}' from entry {entry.get('ID', 'unknown')}")
            return sentence
            
        except Exception as e:
            logger.error(f"Failed to create embedding sentence: {e}")
            # Fallback to basic combination
            return self._fallback_sentence(entry)
    
    def _get_field_value(self, entry: Dict[str, Any], field_names: List[str]) -> str:
        """Get field value, trying multiple possible field names."""
        for field_name in field_names:
            if field_name in entry:
                value = entry[field_name]
                if value and not self._is_na(value):
                    return str(value).strip()
        return ""
    
    def _is_na(self, value) -> bool:
        """Check if value is NaN or None."""
        if value is None:
            return True
        if isinstance(value, float):
            try:
                import math
                return math.isnan(value)
            except:
                return str(value).lower() in ['nan', 'none']
        return False
    
    def _process_filename(self, filename: str) -> str:
        """Extract meaningful terms from filename."""
        if not filename:
            return ""
        
        # Get just the filename without path
        filename = os.path.basename(filename)
        
        # Remove file extension
        filename = os.path.splitext(filename)[0]
        
        # Clean the filename
        cleaned = self._clean_text(filename)
        
        return cleaned
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Convert to string and clean
        text = str(text).strip()
        
        # Remove weird characters like # that showed up in your data
        text = re.sub(r'[#\[\]{}()"]', '', text)
        
        # Replace underscores, hyphens, and dots with spaces
        text = re.sub(r'[_\-\.]', ' ', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.lower()
    
    def _process_description(self, description: str) -> str:
        """Process description text to be more semantic."""
        desc = self._clean_text(description)
        if not desc:
            return ""
        
        # Convert technical terms to more natural language
        replacements = {
            'freq': 'frequency',
            'hz': 'hertz',
            'db': 'decibel',
            'eq': 'equalized',
            'fx': 'effects',
            'reverb': 'reverberation',
            'dist': 'distortion',
            'comp': 'compression'
        }
        
        for abbrev, full in replacements.items():
            desc = re.sub(r'\b' + abbrev + r'\b', full, desc)
        
        return desc
    
    def _normalize_sentence(self, sentence: str) -> str:
        """Final normalization of the embedding sentence."""
        if not sentence:
            return ""
        
        # Clean up the sentence
        sentence = re.sub(r'\s+', ' ', sentence)  # Multiple spaces
        sentence = re.sub(r'[^\w\s]', ' ', sentence)  # Non-alphanumeric except spaces
        sentence = re.sub(r'\s+', ' ', sentence)  # Clean up again
        
        return sentence.strip().lower()
    
    def _fallback_sentence(self, entry: Dict[str, Any]) -> str:
        """Fallback sentence creation for error cases."""
        try:
            # Try description first
            desc = self._get_field_value(entry, ['description', 'Description'])
            if desc:
                return self._clean_text(desc)
            
            # Try filename
            filename = self._get_field_value(entry, ['file', 'File'])
            if filename:
                return self._process_filename(filename)
            
            # Last resort
            return "audio sample"
        except:
            return "audio sample"
    
    def enhance_query(self, query: str) -> str:
        """Enhance user queries for better matching."""
        query = self._clean_text(query)
        if not query:
            return ""
        
        # Expand common abbreviations
        expansions = {
            'fx': 'effects',
            'perc': 'percussion',
            'amb': 'ambient',
            'atmo': 'atmosphere',
            'synth': 'synthesizer'
        }
        
        for abbrev, expansion in expansions.items():
            query = re.sub(r'\b' + abbrev + r'\b', expansion, query)
        
        return query
    
    def extract_keywords(self, text: str) -> List[str]:
        """Extract meaningful keywords from text."""
        text = self._clean_text(text)
        if not text:
            return []
        
        # Split into words and filter
        words = text.split()
        
        # Remove common stop words that don't help with audio search
        stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'this', 'that', 'these', 'those'
        }
        
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
    def create_enhanced_embedding_sentence(self, entry: Dict[str, Any]) -> str:
        """
        Create an enhanced embedding sentence using all available metadata.
        This version intelligently combines multiple fields for richer search.
        """
        try:
            parts = []
            
            # Core content fields
            title = self._get_field_value(entry, ['title', 'Title'])
            description = self._get_field_value(entry, ['description', 'Description'])
            filename = self._get_field_value(entry, ['file', 'File'])
            
            # Add title
            if title:
                cleaned_title = self._clean_text(title)
                if cleaned_title:
                    parts.append(cleaned_title)
            
            # Add description 
            if description:
                cleaned_desc = self._process_description(description)
                if cleaned_desc:
                    parts.append(cleaned_desc)
            
            # Extract semantic info from other fields
            semantic_parts = self._extract_semantic_info(entry)
            if semantic_parts:
                parts.extend(semantic_parts)
            
            # Add filename context if needed
            if len(' '.join(parts)) < 15:  # If still lacking content
                if filename:
                    cleaned_filename = self._process_filename(filename)
                    if cleaned_filename:
                        parts.append(cleaned_filename)
            
            # Combine and clean
            sentence = ' '.join(parts)
            sentence = self._normalize_sentence(sentence)
            
            # Limit length (embeddings work better with focused content)
            words = sentence.split()
            if len(words) > 20:
                sentence = ' '.join(words[:20])
            
            logger.debug(f"Created enhanced embedding: '{sentence}' from entry {entry.get('ID', 'unknown')}")
            return sentence
            
        except Exception as e:
            logger.error(f"Failed to create enhanced embedding: {e}")
            return self.create_embedding_sentence(entry)  # Fallback to basic
    
    def _extract_semantic_info(self, entry: Dict[str, Any]) -> List[str]:
        """Extract semantic information from metadata fields."""
        semantic_parts = []
        
        # Look for type/genre information
        type_info = self._get_field_value(entry, ['type', 'Type', 'genre', 'Genre', 'category', 'Category'])
        if type_info and type_info.lower() not in ['sample', 'audio', 'sound']:
            cleaned_type = self._clean_text(type_info)
            if cleaned_type and len(cleaned_type) > 2:
                semantic_parts.append(cleaned_type)
        
        # Look for instrument information
        instrument = self._get_field_value(entry, ['instrument', 'Instrument', 'source', 'Source'])
        if instrument:
            cleaned_instrument = self._clean_text(instrument)
            if cleaned_instrument and len(cleaned_instrument) > 2:
                semantic_parts.append(cleaned_instrument)
        
        # Look for mood/style descriptors in various fields
        mood_fields = ['mood', 'style', 'feeling', 'vibe', 'character', 'texture']
        for field in mood_fields:
            mood = self._get_field_value(entry, [field, field.capitalize()])
            if mood:
                cleaned_mood = self._clean_text(mood)
                if cleaned_mood and len(cleaned_mood) > 2:
                    semantic_parts.append(cleaned_mood)
        
        # Look for tempo/energy descriptors
        tempo = self._get_field_value(entry, ['tempo', 'Tempo', 'bpm', 'BPM', 'speed', 'Speed'])
        if tempo:
            tempo_desc = self._process_tempo_info(tempo)
            if tempo_desc:
                semantic_parts.append(tempo_desc)
        
        return semantic_parts[:3]  # Limit to top 3 semantic additions
    
    def _process_tempo_info(self, tempo_value: str) -> str:
        """Convert tempo information to descriptive terms."""
        tempo_str = str(tempo_value).lower().strip()
        
        # If it's a number, convert to descriptive terms
        try:
            bpm = float(tempo_str)
            if bpm < 60:
                return "very slow"
            elif bpm < 90:
                return "slow"
            elif bpm < 120:
                return "moderate"
            elif bpm < 140:
                return "fast"
            elif bpm < 180:
                return "very fast"
            else:
                return "extremely fast"
        except ValueError:
            # If it's already descriptive text, clean it
            cleaned = self._clean_text(tempo_str)
            if cleaned in ['slow', 'fast', 'medium', 'moderate', 'quick', 'sluggish']:
                return cleaned
        
        return ""
    
    def generate_smart_embedding_text(self, entry: Dict[str, Any]) -> str:
        """
        Generate smart embedding text that adapts based on available data.
        This is the most intelligent version that considers data quality.
        """
        try:
            # Assess what data we have
            title = self._get_field_value(entry, ['title', 'Title'])
            description = self._get_field_value(entry, ['description', 'Description'])
            filename = self._get_field_value(entry, ['file', 'File'])
            
            title_quality = len(title.split()) if title else 0
            desc_quality = len(description.split()) if description else 0
            
            # Strategy 1: Rich description available
            if desc_quality >= 3:
                return self.create_enhanced_embedding_sentence(entry)
            
            # Strategy 2: Good title, poor description
            elif title_quality >= 2:
                parts = []
                if title:
                    parts.append(self._clean_text(title))
                if description:
                    parts.append(self._process_description(description))
                
                # Add semantic info to compensate for poor description
                semantic_parts = self._extract_semantic_info(entry)
                parts.extend(semantic_parts)
                
                sentence = ' '.join(parts)
                return self._normalize_sentence(sentence)
            
            # Strategy 3: Poor title and description, rely on filename + metadata
            else:
                parts = []
                if title:
                    parts.append(self._clean_text(title))
                if description:
                    parts.append(self._process_description(description))
                if filename:
                    parts.append(self._process_filename(filename))
                
                # Add lots of semantic info
                semantic_parts = self._extract_semantic_info(entry)
                parts.extend(semantic_parts[:5])  # More semantic info
                
                sentence = ' '.join(parts)
                return self._normalize_sentence(sentence)
            
        except Exception as e:
            logger.error(f"Smart embedding generation failed: {e}")
            return self.create_embedding_sentence(entry)  # Fallback
    
    def regenerate_embeddings_for_collection(self, db_manager, embedding_manager, 
                                           strategy: str = "smart") -> Dict[str, int]:
        """
        Utility to regenerate embeddings for entire collection with better text.
        
        Args:
            db_manager: DatabaseManager instance
            embedding_manager: EmbeddingManager instance  
            strategy: "basic", "enhanced", or "smart"
        
        Returns:
            Stats dictionary with counts
        """
        logger.info(f"Regenerating embeddings with '{strategy}' strategy")
        
        stats = {
            'processed': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        try:
            # Get all active entries
            entries = list(db_manager.collection.find({"deleted": {"$ne": True}}))
            
            for entry in entries:
                try:
                    stats['processed'] += 1
                    entry_id = entry.get('ID')
                    
                    # Generate new embedding text based on strategy
                    if strategy == "enhanced":
                        new_text = self.create_enhanced_embedding_sentence(entry)
                    elif strategy == "smart":
                        new_text = self.generate_smart_embedding_text(entry)
                    else:  # basic
                        new_text = self.create_embedding_sentence(entry)
                    
                    # Check if it's different from existing
                    current_text = entry.get('embedding_text', '')
                    if new_text == current_text:
                        stats['skipped'] += 1
                        continue
                    
                    # Create new embedding
                    faiss_id, is_duplicate = embedding_manager.add_embedding(new_text)
                    
                    if faiss_id is not None and not is_duplicate:
                        # Update database
                        if db_manager.update_embedding_info(entry_id, faiss_id, new_text):
                            stats['updated'] += 1
                            logger.info(f"Updated embedding for ID {entry_id}")
                        else:
                            stats['errors'] += 1
                    else:
                        stats['skipped'] += 1
                        
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Failed to update embedding for entry {entry.get('ID', 'unknown')}: {e}")
            
            logger.info(f"Embedding regeneration complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Embedding regeneration failed: {e}")
            return stats