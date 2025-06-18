"""
Text Processor for Hibikidō (Updated)
=====================================

Creates optimized embedding text from hierarchical context.
Priority: segment > segmentation > recording (local > broader)
Target: 15 words, hard limit: 20 words
"""

import re
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Try to import spaCy, fallback to simple processing if not available
try:
    import spacy
    SPACY_AVAILABLE = True
    logger.info("spaCy available for text processing")
except ImportError:
    SPACY_AVAILABLE = False
    logger.info("spaCy not available, using simple text processing")

class TextProcessor:
    def __init__(self):
        self.nlp = None
        
        # Initialize spaCy if available
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
                logger.info("spaCy model loaded successfully")
            except OSError:
                logger.warning("spaCy model 'en_core_web_sm' not found, falling back to simple processing")
                # Don't modify global SPACY_AVAILABLE, just note locally
                self.spacy_working = False
            else:
                self.spacy_working = True
        else:
            self.spacy_working = False
        
        # Audio-specific stop words to remove (in addition to standard ones)
        self.audio_stop_words = {
            'sound', 'audio', 'recording', 'sample', 'track', 'file', 'piece'
        }
    
    def create_segment_embedding_text(self, segment: Dict[str, Any], 
                                    recording: Dict[str, Any] = None,
                                    segmentation: Dict[str, Any] = None) -> str:
        """
        Create embedding text for a segment using hierarchical context.
        Priority: segment > segmentation > recording
        """
        try:
            # Collect context in priority order
            contexts = []
            
            # Priority 1: Segment description (local)
            segment_desc = segment.get("description", "")
            if segment_desc:
                contexts.append(("segment", segment_desc, 10))  # Up to 10 words
            
            # Priority 2: Segmentation description (method context)
            if segmentation:
                seg_desc = segmentation.get("description", "")
                if seg_desc:
                    contexts.append(("segmentation", seg_desc, 5))  # Up to 5 words
            
            # Priority 3: Recording description (broader context)
            if recording:
                rec_desc = recording.get("description", "")
                if rec_desc:
                    contexts.append(("recording", rec_desc, 5))  # Up to 5 words
            
            # Process and combine
            final_text = self._combine_contexts(contexts, target_words=15, max_words=20)
            
            logger.debug(f"Segment embedding text: '{final_text}' for {segment.get('_id', 'unknown')}")
            return final_text
            
        except Exception as e:
            logger.error(f"Failed to create segment embedding text: {e}")
            # Fallback to just segment description
            return self._clean_text(segment.get("description", "audio segment"))
    
    def create_preset_embedding_text(self, preset: Dict[str, Any],
                                   effect: Dict[str, Any] = None) -> str:
        """
        Create embedding text for an effect preset.
        Priority: preset > effect
        """
        try:
            contexts = []
            
            # Priority 1: Preset description (local)
            preset_desc = preset.get("description", "")
            if preset_desc:
                contexts.append(("preset", preset_desc, 12))  # Up to 12 words
            
            # Priority 2: Effect description (broader context)
            if effect:
                effect_desc = effect.get("description", "")
                if effect_desc:
                    contexts.append(("effect", effect_desc, 8))  # Up to 8 words
            
            # Process and combine
            final_text = self._combine_contexts(contexts, target_words=15, max_words=20)
            
            logger.debug(f"Preset embedding text: '{final_text}' for preset in {effect.get('_id', 'unknown') if effect else 'unknown'}")
            return final_text
            
        except Exception as e:
            logger.error(f"Failed to create preset embedding text: {e}")
            # Fallback to just preset description
            return self._clean_text(preset.get("description", "effect preset"))
    
    def _combine_contexts(self, contexts: List[tuple], target_words: int = 15, max_words: int = 20) -> str:
        """
        Combine contexts intelligently, respecting word limits and priorities.
        
        Args:
            contexts: List of (type, text, max_words) tuples in priority order
            target_words: Aim for this many words
            max_words: Hard limit
        """
        if not contexts:
            return ""
        
        combined_words = []
        
        # First pass: take words according to priority and limits
        for context_type, text, word_limit in contexts:
            if len(combined_words) >= max_words:
                break
            
            # Process the text
            words = self._extract_keywords(text, word_limit)
            
            # Add words up to our limits
            remaining_budget = max_words - len(combined_words)
            words_to_add = words[:min(word_limit, remaining_budget)]
            
            combined_words.extend(words_to_add)
        
        # If we're under target, try to add more from available contexts
        if len(combined_words) < target_words:
            for context_type, text, original_limit in contexts:
                if len(combined_words) >= max_words:
                    break
                
                # Get more words from this context
                all_words = self._extract_keywords(text, max_words)
                existing_from_this_context = len([w for w in combined_words 
                                                if w in all_words[:original_limit]])
                
                # Add additional words beyond original limit
                additional_words = all_words[original_limit:]
                remaining_budget = min(target_words - len(combined_words), 
                                     max_words - len(combined_words))
                
                combined_words.extend(additional_words[:remaining_budget])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_words = []
        for word in combined_words:
            if word not in seen:
                seen.add(word)
                unique_words.append(word)
        
        return " ".join(unique_words[:max_words])
    
    def _extract_keywords(self, text: str, max_words: int = None) -> List[str]:
        """Extract meaningful keywords from text."""
        if not text:
            return []
        
        if self.nlp and self.spacy_working:
            return self._extract_keywords_spacy(text, max_words)
        else:
            return self._extract_keywords_simple(text, max_words)
    
    def _extract_keywords_spacy(self, text: str, max_words: int = None) -> List[str]:
        """Extract keywords using spaCy."""
        try:
            doc = self.nlp(text.lower())
            
            keywords = []
            for token in doc:
                # Skip stop words, punctuation, spaces
                if (token.is_stop or token.is_punct or token.is_space or 
                    len(token.text) < 2 or token.text in self.audio_stop_words):
                    continue
                
                # Use lemma for better semantic matching
                lemma = token.lemma_.strip()
                if lemma and len(lemma) > 1:
                    keywords.append(lemma)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_keywords = []
            for word in keywords:
                if word not in seen:
                    seen.add(word)
                    unique_keywords.append(word)
            
            return unique_keywords[:max_words] if max_words else unique_keywords
            
        except Exception as e:
            logger.warning(f"spaCy processing failed: {e}, falling back to simple")
            return self._extract_keywords_simple(text, max_words)
    
    def _extract_keywords_simple(self, text: str, max_words: int = None) -> List[str]:
        """Extract keywords using simple text processing."""
        # Clean and split
        cleaned = self._clean_text(text)
        words = cleaned.split()
        
        # Simple stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
            'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
            'should', 'may', 'might', 'this', 'that', 'these', 'those'
        }
        
        # Combine with audio stop words
        all_stop_words = stop_words | self.audio_stop_words
        
        # Filter meaningful words
        keywords = []
        for word in words:
            if len(word) > 2 and word not in all_stop_words:
                keywords.append(word)
        
        return keywords[:max_words] if max_words else keywords
    
    def _clean_text(self, text: str) -> str:
        """Basic text cleaning."""
        if not text:
            return ""
        
        # Convert to lowercase
        text = str(text).lower().strip()
        
        # Remove special characters, keep alphanumeric and spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def enhance_query(self, query: str) -> str:
        """Enhance user queries for better matching."""
        if not query:
            return ""
        
        # Simple query enhancement - just clean it
        keywords = self._extract_keywords(query, max_words=10)
        return " ".join(keywords)
    
    # Legacy methods for backward compatibility
    def create_embedding_sentence(self, entry: Dict[str, Any]) -> str:
        """Legacy method - creates embedding from single entry."""
        description = entry.get("description", "")
        if description:
            return self._clean_text(description)
        
        # Try other fields
        title = entry.get("title", "")
        if title:
            return self._clean_text(title)
        
        return "audio content"