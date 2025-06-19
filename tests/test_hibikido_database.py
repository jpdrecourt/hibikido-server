"""
Test Suite for Hibikidō Database Manager (Updated for Path-based Schema)
========================================================================

Comprehensive tests for the hierarchical database schema with path-based references.
Run with: python -m pytest test_hibikido_database.py -v
"""

import pytest
import logging
from datetime import datetime
from typing import Dict, Any, List
import uuid

from hibikido.database_manager import HibikidoDatabase

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

class TestHibikidoDatabase:
    """Test class for the path-based hierarchical database."""
    
    @pytest.fixture(scope="function")
    def db(self):
        """Create a test database instance."""
        # Use a test database to avoid conflicts
        test_db = HibikidoDatabase(db_name="hibikido_test")
        
        if not test_db.connect():
            pytest.skip("MongoDB not available")
        
        yield test_db
        
        # Cleanup: drop the test database
        test_db.client.drop_database("hibikido_test")
        test_db.close()
    
    @pytest.fixture
    def sample_recording_path(self):
        """Generate a unique recording path for tests."""
        return f"test/recordings/sample_{uuid.uuid4().hex[:8]}.wav"
    
    @pytest.fixture
    def sample_effect_path(self):
        """Generate a unique effect path for tests."""
        return f"test/effects/sample_{uuid.uuid4().hex[:8]}.maxpat"
    
    def test_database_connection(self, db):
        """Test basic database connection and initialization."""
        assert db.client is not None
        assert db.db is not None
        assert db.recordings is not None
        assert db.segments is not None
        assert db.effects is not None
        assert db.presets is not None  # Now separate collection
        assert db.performances is not None
        assert db.segmentations is not None
    
    # RECORDINGS TESTS (path-based)
    
    def test_add_recording(self, db, sample_recording_path):
        """Test adding a new recording with path as identifier."""
        result = db.add_recording(
            path=sample_recording_path,
            description="Birds singing in the morning forest"
        )
        assert result is True
        
        # Verify it was added
        recording = db.get_recording_by_path(sample_recording_path)
        assert recording is not None
        assert recording["path"] == sample_recording_path
        assert recording["description"] == "Birds singing in the morning forest"
        assert "created_at" in recording
    
    def test_add_duplicate_recording(self, db, sample_recording_path):
        """Test that duplicate recording paths are rejected."""
        # Add first recording
        db.add_recording(sample_recording_path, "Description 1")
        
        # Try to add duplicate
        result = db.add_recording(sample_recording_path, "Description 2")
        assert result is False
    
    def test_get_all_recordings(self, db):
        """Test retrieving all recordings."""
        # Add multiple recordings
        paths = [f"test/rec_{i}.wav" for i in range(3)]
        for i, path in enumerate(paths):
            db.add_recording(path, f"Description {i}")
        
        recordings = db.get_all_recordings()
        assert len(recordings) >= 3
        
        # Check that our recordings are in the results
        found_paths = [r["path"] for r in recordings]
        for path in paths:
            assert path in found_paths
    
    # SEGMENTS TESTS (reference by source_path)
    
    def test_add_segment(self, db, sample_recording_path):
        """Test adding a segment with path-based reference."""
        # Setup recording first
        db.add_recording(sample_recording_path, "Test recording")
        
        # Add segment
        result = db.add_segment(
            source_path=sample_recording_path,
            segmentation_id="manual",
            start=0.1,
            end=0.6,
            description="High-pitched bird call",
            embedding_text="high pitched bird call morning forest",
            faiss_index=42
        )
        assert result is True
        
        # Verify segment was added
        segments = db.get_segments_by_recording_path(sample_recording_path)
        assert len(segments) == 1
        
        segment = segments[0]
        assert segment["source_path"] == sample_recording_path
        assert segment["start"] == 0.1
        assert segment["end"] == 0.6
        assert segment["FAISS_index"] == 42
    
    def test_get_segment_by_faiss_id(self, db, sample_recording_path):
        """Test retrieving segment by FAISS index."""
        # Setup
        db.add_recording(sample_recording_path, "Test recording")
        
        faiss_index = 123
        db.add_segment(
            source_path=sample_recording_path,
            segmentation_id="manual",
            start=0.0,
            end=0.5,
            description="Test segment",
            embedding_text="test segment",
            faiss_index=faiss_index
        )
        
        # Retrieve by FAISS index
        segment = db.get_segment_by_faiss_id(faiss_index)
        assert segment is not None
        assert segment["FAISS_index"] == faiss_index
        assert segment["source_path"] == sample_recording_path
    
    def test_get_segments_by_recording_path(self, db, sample_recording_path):
        """Test getting all segments for a recording by path."""
        # Setup
        db.add_recording(sample_recording_path, "Test recording")
        
        # Add multiple segments
        segment_data = []
        for i in range(3):
            success = db.add_segment(
                source_path=sample_recording_path,
                segmentation_id="manual",
                start=i * 0.3,
                end=(i + 1) * 0.3,
                description=f"Segment {i}",
                embedding_text=f"segment {i}"
            )
            assert success, f"Failed to add segment {i}"
        
        # Retrieve segments
        segments = db.get_segments_by_recording_path(sample_recording_path)
        assert len(segments) == 3
        
        # Check they're sorted by start time
        for i in range(len(segments) - 1):
            assert segments[i]["start"] <= segments[i + 1]["start"]
    
    def test_normalized_segment_values(self, db, sample_recording_path):
        """Test that segments store normalized 0-1 values."""
        # Setup
        db.add_recording(sample_recording_path, "Test recording")
        
        # Add segment with normalized values
        db.add_segment(
            source_path=sample_recording_path,
            segmentation_id="manual",
            start=0.25,  # Normalized values
            end=0.75,
            description="Middle section",
            embedding_text="middle section"
        )
        
        segments = db.get_segments_by_recording_path(sample_recording_path)
        segment = segments[0]
        
        assert segment["start"] == 0.25
        assert segment["end"] == 0.75
        assert 0.0 <= segment["start"] <= 1.0
        assert 0.0 <= segment["end"] <= 1.0
    
    # EFFECTS TESTS (path-based)
    
    def test_add_effect(self, db, sample_effect_path):
        """Test adding an effect with path as identifier."""
        result = db.add_effect(
            path=sample_effect_path,
            name="Granular Delay",
            description="Granular synthesis delay with pitch shifting"
        )
        assert result is True
        
        # Verify it was added
        effect = db.get_effect_by_path(sample_effect_path)
        assert effect is not None
        assert effect["name"] == "Granular Delay"
        assert effect["path"] == sample_effect_path
        assert effect["description"] == "Granular synthesis delay with pitch shifting"
    
    def test_add_duplicate_effect(self, db, sample_effect_path):
        """Test that duplicate effect paths are rejected."""
        # Add first effect
        db.add_effect(sample_effect_path, "Effect 1", "Description 1")
        
        # Try to add duplicate
        result = db.add_effect(sample_effect_path, "Effect 2", "Description 2")
        assert result is False
    
    # PRESETS TESTS (separate collection, reference by effect_path)
    
    def test_add_preset(self, db, sample_effect_path):
        """Test adding a preset to separate presets collection."""
        # Add effect first
        db.add_effect(sample_effect_path, "Test Effect", "Test effect description")
        
        # Add preset
        result = db.add_preset(
            effect_path=sample_effect_path,
            parameters=[0.5, 0.7, 0.1],
            description="Ethereal long delay with small grains",
            embedding_text="ethereal long delay small grains atmospheric",
            faiss_index=456
        )
        assert result is True
        
        # Verify preset was added
        presets = db.get_presets_by_effect_path(sample_effect_path)
        assert len(presets) == 1
        
        preset = presets[0]
        assert preset["effect_path"] == sample_effect_path
        assert preset["description"] == "Ethereal long delay with small grains"
        assert preset["FAISS_index"] == 456
        assert preset["parameters"] == [0.5, 0.7, 0.1]
    
    def test_get_preset_by_faiss_id(self, db, sample_effect_path):
        """Test retrieving preset by FAISS index."""
        # Setup
        db.add_effect(sample_effect_path, "Test Effect", "Test description")
        
        faiss_index = 789
        db.add_preset(
            effect_path=sample_effect_path,
            parameters=[0.5],
            description="Test preset",
            embedding_text="test preset",
            faiss_index=faiss_index
        )
        
        # Retrieve by FAISS index
        preset = db.get_preset_by_faiss_id(faiss_index)
        assert preset is not None
        assert preset["FAISS_index"] == faiss_index
        assert preset["effect_path"] == sample_effect_path
        assert preset["description"] == "Test preset"
    
    def test_get_presets_by_effect_path(self, db, sample_effect_path):
        """Test getting all presets for an effect by path."""
        # Setup
        db.add_effect(sample_effect_path, "Test Effect", "Test description")
        
        # Add multiple presets
        for i in range(3):
            success = db.add_preset(
                effect_path=sample_effect_path,
                parameters=[i * 0.1],
                description=f"Preset {i}",
                embedding_text=f"preset {i}"
            )
            assert success, f"Failed to add preset {i}"
        
        # Retrieve presets
        presets = db.get_presets_by_effect_path(sample_effect_path)
        assert len(presets) == 3
        
        # Check all reference the same effect
        for preset in presets:
            assert preset["effect_path"] == sample_effect_path
    
    def test_presets_without_embeddings(self, db, sample_effect_path):
        """Test finding presets without FAISS embeddings."""
        # Setup
        db.add_effect(sample_effect_path, "Test Effect", "Test description")
        
        # Add preset without FAISS index
        db.add_preset(
            effect_path=sample_effect_path,
            parameters=[0.5],
            description="No embedding preset",
            embedding_text="no embedding preset"
            # Note: no faiss_index parameter
        )
        
        # Find presets without embeddings
        presets = db.get_presets_without_embeddings()
        assert len(presets) >= 1
        
        # Check our preset is in the results
        found = False
        for preset in presets:
            if (preset["effect_path"] == sample_effect_path and 
                preset["description"] == "No embedding preset"):
                found = True
                break
        assert found
    
    # STATISTICS TESTS
    
    def test_get_stats(self, db, sample_recording_path, sample_effect_path):
        """Test getting comprehensive database statistics."""
        # Add test data
        db.add_recording(sample_recording_path, "Test recording")
        
        # Add segment with embedding
        db.add_segment(
            source_path=sample_recording_path,
            segmentation_id="manual",
            start=0.0,
            end=0.5,
            description="Test segment",
            embedding_text="test segment",
            faiss_index=123
        )
        
        # Add effect with preset
        db.add_effect(sample_effect_path, "Test Effect", "Test description")
        db.add_preset(
            effect_path=sample_effect_path,
            parameters=[0.5],
            description="Test preset",
            embedding_text="test preset",
            faiss_index=456
        )
        
        # Add performance
        performance_id = f"perf_{uuid.uuid4().hex[:8]}"
        db.add_performance(performance_id)
        
        # Get stats
        stats = db.get_stats()
        
        assert stats["recordings"] >= 1
        assert stats["segments"] >= 1
        assert stats["segments_with_embeddings"] >= 1
        assert stats["effects"] >= 1
        assert stats["presets"] >= 1  # Now separate collection
        assert stats["presets_with_embeddings"] >= 1
        assert stats["performances"] >= 1
        assert stats["total_searchable_items"] >= 2  # segment + preset
    
    # INTEGRATION TESTS
    
    def test_full_path_based_workflow(self, db):
        """Test a complete workflow using path-based references."""
        # 1. Add recordings
        recording_paths = [
            "sounds/forest/morning_birds.wav",
            "sounds/city/traffic_ambience.wav"
        ]
        
        for path in recording_paths:
            success = db.add_recording(
                path=path,
                description=f"Recording: {path.split('/')[-1]}"
            )
            assert success, f"Failed to add recording {path}"
        
        # 2. Add segments referencing by path
        segments_data = [
            {
                "source_path": "sounds/forest/morning_birds.wav",
                "start": 0.0, "end": 0.3,
                "description": "Robin territorial call",
                "embedding_text": "robin territorial call bright morning",
                "faiss_index": 100
            },
            {
                "source_path": "sounds/forest/morning_birds.wav", 
                "start": 0.4, "end": 0.8,
                "description": "Blackbird melodic song",
                "embedding_text": "blackbird melodic song complex musical",
                "faiss_index": 101
            },
            {
                "source_path": "sounds/city/traffic_ambience.wav",
                "start": 0.0, "end": 1.0,
                "description": "Urban traffic flow",
                "embedding_text": "urban traffic flow constant rumble",
                "faiss_index": 102
            }
        ]
        
        added_segments = []
        for seg in segments_data:
            success = db.add_segment(
                source_path=seg["source_path"],
                segmentation_id="manual",
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"],
                faiss_index=seg["faiss_index"]
            )
            assert success, f"Failed to add segment {seg['description']}"
            added_segments.append(seg)
        
        # 3. Add effects
        effect_paths = [
            "effects/reverb/cathedral.maxpat",
            "effects/granular/processor.maxpat"
        ]
        
        for path in effect_paths:
            success = db.add_effect(
                path=path,
                name=path.split('/')[-1].split('.')[0],
                description=f"Effect: {path}"
            )
            assert success, f"Failed to add effect {path}"
        
        # 4. Add presets referencing by effect path
        presets_data = [
            {
                "effect_path": "effects/reverb/cathedral.maxpat",
                "parameters": [0.8, 0.3, 0.9],
                "description": "Warm cathedral reverb",
                "embedding_text": "warm cathedral reverb spacious sacred",
                "faiss_index": 200
            },
            {
                "effect_path": "effects/granular/processor.maxpat",
                "parameters": [0.05, 0.2, 2.0],
                "description": "Ethereal time stretch",
                "embedding_text": "ethereal time stretch atmospheric slow",
                "faiss_index": 201
            }
        ]
        
        added_presets = []
        for preset in presets_data:
            success = db.add_preset(
                effect_path=preset["effect_path"],
                parameters=preset["parameters"],
                description=preset["description"],
                embedding_text=preset["embedding_text"],
                faiss_index=preset["faiss_index"]
            )
            assert success, f"Failed to add preset {preset['description']}"
            added_presets.append(preset)
        
        # 5. Verify path-based relationships
        # Check segments for forest recording
        forest_segments = db.get_segments_by_recording_path("sounds/forest/morning_birds.wav")
        assert len(forest_segments) == 2
        
        # Check presets for reverb effect
        reverb_presets = db.get_presets_by_effect_path("effects/reverb/cathedral.maxpat")
        assert len(reverb_presets) == 1
        
        # 6. Verify FAISS lookups work
        robin_segment = db.get_segment_by_faiss_id(100)
        assert robin_segment is not None
        assert robin_segment["description"] == "Robin territorial call"
        assert robin_segment["source_path"] == "sounds/forest/morning_birds.wav"
        
        cathedral_preset = db.get_preset_by_faiss_id(200)
        assert cathedral_preset is not None
        assert cathedral_preset["description"] == "Warm cathedral reverb"
        assert cathedral_preset["effect_path"] == "effects/reverb/cathedral.maxpat"
        
        # 7. Check stats
        stats = db.get_stats()
        assert stats["recordings"] >= 2
        assert stats["segments"] >= 3
        assert stats["effects"] >= 2
        assert stats["presets"] >= 2
        assert stats["total_searchable_items"] >= 5
        
        print("✅ Full path-based workflow test passed!")


# UTILITY TESTS AND FIXTURES

class TestDataFixtures:
    """Create realistic test data for manual testing with path-based schema."""
    
    @staticmethod
    def create_bird_recording_set(db: HibikidoDatabase):
        """Create a realistic set of bird recording data."""
        # Recording with path
        recording_path = "recordings/field/forest_dawn_001.wav"
        db.add_recording(
            path=recording_path,
            description="Dawn chorus recorded in oak woodland, spring morning"
        )
        
        # Segmentation
        segmentation_id = "manual_bird_calls_v1"
        db.add_segmentation(
            segmentation_id=segmentation_id,
            method="manual",
            parameters={"min_duration": 0.3, "focus": "bird_calls"},
            description="Hand-segmented individual bird vocalizations"
        )
        
        # Segments (normalized 0-1 values)
        segments = [
            {
                "id": "robin_territorial_001",
                "start": 0.05, "end": 0.35,
                "description": "Robin territorial call, bright and assertive",
                "embedding_text": "robin territorial call bright assertive morning"
            },
            {
                "id": "blackbird_song_001", 
                "start": 0.45, "end": 0.85,
                "description": "Blackbird melodic song, complex phrases",
                "embedding_text": "blackbird melodic song complex phrases musical"
            },
            {
                "id": "wren_trill_001",
                "start": 0.90, "end": 1.0,
                "description": "Wren rapid trill, high frequency",
                "embedding_text": "wren rapid trill high frequency energetic"
            }
        ]
        
        for i, seg in enumerate(segments):
            db.add_segment(
                source_path=recording_path,  # Reference by path
                segmentation_id=segmentation_id,
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"],
                faiss_index=1000 + i
            )
        
        return recording_path, segmentation_id, [s["id"] for s in segments]
    
    @staticmethod
    def create_granular_effects(db: HibikidoDatabase):
        """Create realistic granular synthesis effects with separate presets."""
        effect_path = "effects/granular/processor_v2.maxpat"
        db.add_effect(
            path=effect_path,
            name="Granular Processor",
            description="Advanced granular synthesis with pitch and time manipulation"
        )
        
        presets = [
            {
                "parameters": [0.05, 0.2, 2.0, 0.7],
                "description": "Ethereal time-stretched texture",
                "embedding_text": "ethereal time stretched texture atmospheric slow",
                "faiss_index": 2000
            },
            {
                "parameters": [0.01, 1.5, 0.5, 0.9],
                "description": "Glitchy fragmented rhythm",
                "embedding_text": "glitchy fragmented rhythm digital chaotic fast",
                "faiss_index": 2001
            },
            {
                "parameters": [0.1, 0.0, 1.0, 0.3],
                "description": "Sparse natural granulation",
                "embedding_text": "sparse natural granulation organic subtle",
                "faiss_index": 2002
            }
        ]
        
        for preset in presets:
            db.add_preset(
                effect_path=effect_path,  # Reference by path
                parameters=preset["parameters"],
                description=preset["description"],
                embedding_text=preset["embedding_text"],
                faiss_index=preset["faiss_index"]
            )
        
        return effect_path


if __name__ == "__main__":
    """Run basic tests manually."""
    import sys
    
    # Basic connection test
    db = HibikidoDatabase(db_name="hibikido_manual_test")
    
    if not db.connect():
        print("❌ Failed to connect to MongoDB")
        sys.exit(1)
    
    print("✅ Connected to MongoDB")
    
    try:
        # Create test data
        print("Creating test data...")
        fixtures = TestDataFixtures()
        
        recording_path, segmentation_id, segment_ids = fixtures.create_bird_recording_set(db)
        effect_path = fixtures.create_granular_effects(db)
        
        # Test retrieval
        print(f"✅ Created recording: {recording_path}")
        print(f"✅ Created {len(segment_ids)} segments")
        print(f"✅ Created effect with presets: {effect_path}")
        
        # Test stats
        stats = db.get_stats()
        print(f"📊 Database stats: {stats}")
        
        # Test path-based lookups
        recording = db.get_recording_by_path(recording_path)
        if recording:
            print(f"✅ Found recording by path: {recording['description']}")
        
        effect = db.get_effect_by_path(effect_path)
        if effect:
            print(f"✅ Found effect by path: {effect['name']}")
        
        # Test FAISS lookups
        robin_segment = db.get_segment_by_faiss_id(1000)
        if robin_segment:
            print(f"✅ Found segment by FAISS ID: {robin_segment['description']}")
        
        ethereal_preset = db.get_preset_by_faiss_id(2000)
        if ethereal_preset:
            print(f"✅ Found preset by FAISS ID: {ethereal_preset['description']}")
        
        # Test relationship queries
        forest_segments = db.get_segments_by_recording_path(recording_path)
        print(f"✅ Found {len(forest_segments)} segments for recording")
        
        granular_presets = db.get_presets_by_effect_path(effect_path)
        print(f"✅ Found {len(granular_presets)} presets for effect")
        
        print("\n🎉 All manual tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        db.client.drop_database("hibikido_manual_test")
        db.close()
        print("🧹 Cleaned up test database")