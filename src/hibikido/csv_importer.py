"""
CSV Importer for Incantation Server
==================================

Handles importing data from CSV files with intelligent text processing.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging

from .text_processor import TextProcessor

logger = logging.getLogger(__name__)

class CSVImporter:
    def __init__(self, text_processor: TextProcessor = None):
        self.text_processor = text_processor or TextProcessor()
    
    def import_csv(self, filepath: str, db_manager, embedding_manager) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Import entries from CSV file.
        Updates existing entries, adds new ones, never re-embeds.
        
        Args:
            filepath: Path to CSV file
            db_manager: DatabaseManager instance
            embedding_manager: EmbeddingManager instance
            
        Returns (entries_list, error_messages)
        """
        try:
            logger.info(f"Importing CSV file: {filepath}")
            
            # Read CSV with multiple encoding attempts
            df = self._read_csv_robust(filepath)
            if df is None:
                return [], ["Failed to read CSV file"]
            
            logger.info(f"Processing {len(df)} rows from CSV")
            
            entries = []
            errors = []
            added_count = 0
            updated_count = 0
            skipped_count = 0
            
            for index, row in df.iterrows():
                try:
                    entry = self._process_row(row, index)
                    if not entry:
                        errors.append(f"Row {index + 1}: Failed to process (missing required data)")
                        continue
                    
                    # Check if entry already exists
                    existing_entry = db_manager.get_by_id(entry['ID'])
                    
                    if existing_entry:
                        # Prepare update data (exclude system fields we want to preserve)
                        updates = {}
                        for key, value in entry.items():
                            if key not in ['_id', 'faiss_id', 'embedding_text', 'created_at', 'deleted', 'deleted_at']:
                                updates[key] = value
                        
                        # Add updated timestamp
                        updates['updated_at'] = datetime.now()
                        
                        # Update using MongoDB's update_one
                        result = db_manager.collection.update_one(
                            {"ID": entry['ID']},
                            {"$set": updates}
                        )
                        
                        if result.modified_count > 0:
                            updated_count += 1
                            logger.info(f"Updated existing entry ID {entry['ID']} - {entry.get('title', 'untitled')}")
                        else:
                            skipped_count += 1
                            logger.debug(f"No changes for entry ID {entry['ID']}")
                    
                    else:
                        # Add new entry with embedding
                        faiss_id, is_duplicate = embedding_manager.add_embedding(
                            entry['embedding_text']
                        )
                        
                        if faiss_id is not None and not is_duplicate:
                            entry['faiss_id'] = faiss_id
                        
                        # Add to database
                        if db_manager.add_entry(entry):
                            added_count += 1
                            entries.append(entry)
                            logger.debug(f"Added new entry ID {entry['ID']}")
                        else:
                            skipped_count += 1
                            logger.warning(f"Failed to add entry ID {entry['ID']}")
                    
                except Exception as e:
                    error_msg = f"Row {index + 1}: {str(e)}"
                    errors.append(error_msg)
                    logger.warning(error_msg)
            
            logger.info(f"CSV import completed: {added_count} added, {updated_count} updated, {skipped_count} skipped")
            return entries, errors
            
        except Exception as e:
            error_msg = f"CSV import failed: {e}"
            logger.error(error_msg)
            return [], [error_msg]

    def _read_csv_robust(self, filepath: str) -> pd.DataFrame:
        """Read CSV with UTF-8 encoding."""
        try:
            logger.info(f"Reading CSV with UTF-8 encoding: {filepath}")
            df = pd.read_csv(filepath, encoding='utf-8')
            logger.info("Successfully read CSV")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return None
    
    def _process_row(self, row: pd.Series, row_index: int) -> Dict[str, Any]:
        """Process a single CSV row into an entry dictionary."""
        try:
            # Map CSV columns to entry fields
            entry = {
                "ID": self._get_int_value(row, ['ID', 'id']),
                "file": self._get_string_value(row, ['File', 'file']),
                "type": self._get_string_value(row, ['Type', 'type']),
                "title": self._get_string_value(row, ['Title', 'title']),
                "description": self._get_string_value(row, ['Description', 'description']),
                "duration": self._get_float_value(row, ['Duration', 'duration']),
                "created_at": datetime.now(),
                "deleted": False,
                "segments": []  # Will hold future segmentation data
            }
            
            # Handle date created if present
            date_created = self._get_string_value(row, ['Date created', 'date_created'])
            if date_created:
                entry["date_created"] = date_created
            
            # Create embedding text using text processor (simplified)
            entry["embedding_text"] = self.text_processor.create_embedding_sentence(entry)
            
            # Validate required fields
            if not entry["ID"]:
                logger.warning(f"Row {row_index + 1}: Missing ID, skipping")
                return None
            
            if not entry["embedding_text"]:
                logger.warning(f"Row {row_index + 1}: No meaningful text for embedding, skipping")
                return None
            
            # Set defaults
            if not entry["type"]:
                entry["type"] = "sample"
            
            if not entry["title"]:
                entry["title"] = entry["file"] or f"Entry {entry['ID']}"
            
            return entry
            
        except Exception as e:
            logger.error(f"Error processing row {row_index + 1}: {e}")
            return None
    
    def _get_string_value(self, row: pd.Series, column_names: List[str]) -> str:
        """Get string value from row, trying multiple column names."""
        for col_name in column_names:
            if col_name in row and not pd.isna(row[col_name]):
                value = str(row[col_name]).strip()
                return value if value else ""
        return ""
    
    def _get_int_value(self, row: pd.Series, column_names: List[str]) -> int:
        """Get integer value from row, trying multiple column names."""
        for col_name in column_names:
            if col_name in row and not pd.isna(row[col_name]):
                try:
                    return int(float(row[col_name]))  # Handle float-formatted integers
                except (ValueError, TypeError):
                    continue
        return 0
    
    def _get_float_value(self, row: pd.Series, column_names: List[str]) -> float:
        """Get float value from row, trying multiple column names."""
        for col_name in column_names:
            if col_name in row and not pd.isna(row[col_name]):
                try:
                    return float(row[col_name])
                except (ValueError, TypeError):
                    continue
        return 0.0
    
    def validate_csv_structure(self, filepath: str) -> Tuple[bool, List[str]]:
        """
        Validate CSV structure before import.
        Returns (is_valid, issues_list)
        """
        try:
            df = self._read_csv_robust(filepath)
            if df is None:
                return False, ["Cannot read CSV file"]
            
            issues = []
            
            # Check for required columns (at least one ID column)
            id_columns = [col for col in df.columns if col.lower() in ['id', 'ID']]
            if not id_columns:
                issues.append("No ID column found (required)")
            
            # Check for meaningful content columns
            content_columns = [col for col in df.columns 
                             if col.lower() in ['title', 'description', 'type', 'file', 'name']]
            if not content_columns:
                issues.append("No content columns found (need at least title, description, or type)")
            
            # Check for duplicate IDs
            if id_columns:
                id_col = id_columns[0]
                duplicates = df[df.duplicated(subset=[id_col], keep=False)]
                if not duplicates.empty:
                    issues.append(f"Found {len(duplicates)} duplicate IDs")
            
            # Check data quality
            if len(df) == 0:
                issues.append("CSV file is empty")
            elif len(df) > 10000:
                issues.append(f"Large file ({len(df)} rows) - consider splitting")
            
            # Summary
            logger.info(f"CSV validation: {len(df)} rows, {len(df.columns)} columns")
            logger.info(f"Columns: {list(df.columns)}")
            
            return len(issues) == 0, issues
            
        except Exception as e:
            return False, [f"Validation error: {e}"]