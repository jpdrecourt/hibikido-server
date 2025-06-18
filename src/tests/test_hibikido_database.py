"""
Test Suite for Hibikidō Database Manager
========================================

Comprehensive tests for the hierarchical database schema.
Run with: python -m pytest test_hibikido_database.py -v
"""

import pytest
import logging
from datetime import datetime
from typing import Dict, Any, List
import uuid

# Assuming the new database manager is in database_manager_v2.py
from database_manager import HibikidoDatabase

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

class TestHibikidoDatabase:
    """Test class for the new hierarchical database."""
    
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
    def sample_recording_id(self):
        """Generate a unique recording ID for tests."""
        return f"rec_{uuid.uuid4().hex[:8]}"
    
    @pytest.fixture
    def sample_segmentation_id(self):
        """Generate a unique segmentation ID for tests."""
        return f"seg_{uuid.uuid4().hex[:8]}"
    
    @pytest.fixture
    def sample_effect_id(self):
        """Generate a unique effect ID for tests."""
        return f"fx_{uuid.uuid4().hex[:8]}"
    
    def test_database_connection(self, db):
        """Test basic database connection and initialization."""
        assert db.client is not None
        assert db.db is not None
        assert db.recordings is not None
        assert db.segments is not None
        assert db.effects is not None
        assert db.performances is not None
        assert db.segmentations is not None
    
    # RECORDINGS TESTS
    
    def test_add_recording(self, db, sample_recording_id):
        """Test adding a new recording."""
        result = db.add_recording(
            recording_id=sample_recording_id,
            path="test/audio/field_recording_001.wav",
            description="Birds singing in the morning forest"
        )
        assert result is True
        
        # Verify it was added
        recording = db.get_recording(sample_recording_id)
        assert recording is not None
        assert recording["path"] == "test/audio/field_recording_001.wav"
        assert recording["description"] == "Birds singing in the morning forest"
        assert "created_at" in recording
    
    def test_add_duplicate_recording(self, db, sample_recording_id):
        """Test that duplicate recording IDs are rejected."""
        # Add first recording
        db.add_recording(sample_recording_id, "path1.wav", "Description 1")
        
        # Try to add duplicate
        result = db.add_recording(sample_recording_id, "path2.wav", "Description 2")
        assert result is False
    
    def test_get_all_recordings(self, db):
        """Test retrieving all recordings."""
        # Add multiple recordings
        rec_ids = [f"rec_{i}" for i in range(3)]
        for i, rec_id in enumerate(rec_ids):
            db.add_recording(rec_id, f"path_{i}.wav", f"Description {i}")
        
        recordings = db.get_all_recordings()
        assert len(recordings) >= 3
        
        # Check that our recordings are in the results
        rec_ids_found = [r["_id"] for r in recordings]
        for rec_id in rec_ids:
            assert rec_id in rec_ids_found
    
    def test_update_recording(self, db, sample_recording_id):
        """Test updating recording metadata."""
        # Add recording
        db.add_recording(sample_recording_id, "original.wav", "Original description")
        
        # Update it
        result = db.update_recording(sample_recording_id, {
            "description": "Updated description",
            "duration": 120.5
        })
        assert result is True
        
        # Verify update
        recording = db.get_recording(sample_recording_id)
        assert recording["description"] == "Updated description"
        assert recording["duration"] == 120.5
        assert "updated_at" in recording
    
    # SEGMENTATIONS TESTS
    
    def test_add_segmentation(self, db, sample_segmentation_id):
        """Test adding a segmentation method."""
        result = db.add_segmentation(
            segmentation_id=sample_segmentation_id,
            method="manual",
            parameters={"threshold": 0.5, "min_duration": 1.0},
            description="Hand-curated segments focusing on bird calls"
        )
        assert result is True
        
        # Verify it was added
        segmentation = db.get_segmentation(sample_segmentation_id)
        assert segmentation is not None
        assert segmentation["method"] == "manual"
        assert segmentation["parameters"]["threshold"] == 0.5
        assert segmentation["description"] == "Hand-curated segments focusing on bird calls"
    
    # SEGMENTS TESTS
    
    def test_add_segment(self, db, sample_recording_id, sample_segmentation_id):
        """Test adding a segment."""
        # Setup dependencies
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        
        # Add segment
        segment_id = f"seg_{uuid.uuid4().hex[:8]}"
        result = db.add_segment(
            segment_id=segment_id,
            source_id=sample_recording_id,
            segmentation_id=sample_segmentation_id,
            start=0.0,
            end=0.25,
            description="High-pitched bird call",
            embedding_text="high pitched bird call morning forest",
            faiss_index=42
        )
        assert result is True
        
        # Verify it was added
        segment = db.get_segment(segment_id)
        assert segment is not None
        assert segment["source_id"] == sample_recording_id
        assert segment["segmentation_id"] == sample_segmentation_id
        assert segment["start"] == 0.0
        assert segment["end"] == 0.25
        assert segment["FAISS_index"] == 42
    
    def test_get_segment_by_faiss_id(self, db, sample_recording_id, sample_segmentation_id):
        """Test retrieving segment by FAISS index."""
        # Setup
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        
        segment_id = f"seg_{uuid.uuid4().hex[:8]}"
        faiss_index = 123
        
        db.add_segment(
            segment_id=segment_id,
            source_id=sample_recording_id,
            segmentation_id=sample_segmentation_id,
            start=0.0,
            end=0.5,
            description="Test segment",
            embedding_text="test segment",
            faiss_index=faiss_index
        )
        
        # Retrieve by FAISS index
        segment = db.get_segment_by_faiss_id(faiss_index)
        assert segment is not None
        assert segment["_id"] == segment_id
        assert segment["FAISS_index"] == faiss_index
    
    def test_get_segments_by_recording(self, db, sample_recording_id, sample_segmentation_id):
        """Test getting all segments for a recording."""
        # Setup
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        
        # Add multiple segments
        segment_ids = []
        for i in range(3):
            segment_id = f"seg_{i}_{uuid.uuid4().hex[:8]}"
            segment_ids.append(segment_id)
            db.add_segment(
                segment_id=segment_id,
                source_id=sample_recording_id,
                segmentation_id=sample_segmentation_id,
                start=i * 0.33,
                end=(i + 1) * 0.33,
                description=f"Segment {i}",
                embedding_text=f"segment {i}"
            )
        
        # Retrieve segments
        segments = db.get_segments_by_recording(sample_recording_id)
        assert len(segments) == 3
        
        # Check they're sorted by start time
        for i in range(len(segments) - 1):
            assert segments[i]["start"] <= segments[i + 1]["start"]
    
    def test_segments_without_embeddings(self, db, sample_recording_id, sample_segmentation_id):
        """Test finding segments without FAISS embeddings."""
        # Setup
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        
        # Add segment without FAISS index
        segment_id = f"seg_{uuid.uuid4().hex[:8]}"
        db.add_segment(
            segment_id=segment_id,
            source_id=sample_recording_id,
            segmentation_id=sample_segmentation_id,
            start=0.0,
            end=0.5,
            description="No embedding yet",
            embedding_text="no embedding"
            # Note: no faiss_index parameter
        )
        
        # Find segments without embeddings
        segments = db.get_segments_without_embeddings()
        segment_ids = [s["_id"] for s in segments]
        assert segment_id in segment_ids
    
    # EFFECTS TESTS
    
    def test_add_effect(self, db, sample_effect_id):
        """Test adding an effect."""
        result = db.add_effect(
            effect_id=sample_effect_id,
            name="Granular Delay",
            path="/effects/granular_delay.maxpat",
            description="Granular synthesis delay with pitch shifting"
        )
        assert result is True
        
        # Verify it was added
        effect = db.get_effect(sample_effect_id)
        assert effect is not None
        assert effect["name"] == "Granular Delay"
        assert effect["path"] == "/effects/granular_delay.maxpat"
        assert effect["presets"] == []
    
    def test_add_preset_to_effect(self, db, sample_effect_id):
        """Test adding a preset to an effect."""
        # Add effect first
        db.add_effect(sample_effect_id, "Test Effect", "/test.maxpat")
        
        # Add preset
        preset = {
            "parameters": [
                {"name": "delay_time", "value": 0.5},
                {"name": "feedback", "value": 0.7},
                {"name": "grain_size", "value": 0.1}
            ],
            "description": "Ethereal long delay with small grains",
            "embedding_text": "ethereal long delay small grains atmospheric",
            "FAISS_index": 456
        }
        
        result = db.add_preset_to_effect(sample_effect_id, preset)
        assert result is True
        
        # Verify preset was added
        effect = db.get_effect(sample_effect_id)
        assert len(effect["presets"]) == 1
        assert effect["presets"][0]["description"] == "Ethereal long delay with small grains"
        assert effect["presets"][0]["FAISS_index"] == 456
        assert len(effect["presets"][0]["parameters"]) == 3
    
    def test_get_preset_by_faiss_id(self, db, sample_effect_id):
        """Test retrieving preset by FAISS index."""
        # Setup
        db.add_effect(sample_effect_id, "Test Effect", "/test.maxpat")
        
        preset = {
            "parameters": [{"name": "param1", "value": 0.5}],
            "description": "Test preset",
            "embedding_text": "test preset",
            "FAISS_index": 789
        }
        
        db.add_preset_to_effect(sample_effect_id, preset)
        
        # Retrieve by FAISS index
        result = db.get_preset_by_faiss_id(789)
        assert result is not None
        
        effect, found_preset = result
        assert effect["_id"] == sample_effect_id
        assert found_preset["FAISS_index"] == 789
        assert found_preset["description"] == "Test preset"
    
    def test_presets_without_embeddings(self, db, sample_effect_id):
        """Test finding presets without FAISS embeddings."""
        # Setup
        db.add_effect(sample_effect_id, "Test Effect", "/test.maxpat")
        
        # Add preset without FAISS index
        preset = {
            "parameters": [{"name": "param1", "value": 0.5}],
            "description": "No embedding preset",
            "embedding_text": "no embedding preset"
            # Note: no FAISS_index
        }
        
        db.add_preset_to_effect(sample_effect_id, preset)
        
        # Find presets without embeddings
        presets = db.get_presets_without_embeddings()
        assert len(presets) >= 1
        
        # Check our preset is in the results
        found = False
        for effect_id, preset_index, preset_data in presets:
            if effect_id == sample_effect_id and preset_data["description"] == "No embedding preset":
                found = True
                break
        assert found
    
    # PERFORMANCES TESTS
    
    def test_add_performance(self, db):
        """Test adding a performance session."""
        performance_id = f"perf_{uuid.uuid4().hex[:8]}"
        test_date = datetime(2024, 1, 15, 14, 30, 0)
        
        result = db.add_performance(performance_id, test_date)
        assert result is True
        
        # Verify it was added
        performance = db.get_performance(performance_id)
        assert performance is not None
        assert performance["date"] == test_date
        assert performance["invocations"] == []
    
    def test_add_invocation(self, db, sample_recording_id, sample_segmentation_id):
        """Test adding invocations to a performance."""
        # Setup
        performance_id = f"perf_{uuid.uuid4().hex[:8]}"
        db.add_performance(performance_id)
        
        # Setup segment for reference
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        segment_id = f"seg_{uuid.uuid4().hex[:8]}"
        db.add_segment(
            segment_id=segment_id,
            source_id=sample_recording_id,
            segmentation_id=sample_segmentation_id,
            start=0.0,
            end=0.5,
            description="Test segment",
            embedding_text="test segment"
        )
        
        # Add invocation
        result = db.add_invocation(
            performance_id=performance_id,
            text="ethereal morning birds",
            time=5.2,
            segment_id=segment_id
        )
        assert result is True
        
        # Verify invocation was added
        performance = db.get_performance(performance_id)
        assert len(performance["invocations"]) == 1
        
        invocation = performance["invocations"][0]
        assert invocation["text"] == "ethereal morning birds"
        assert invocation["time"] == 5.2
        assert invocation["segment_id"] == segment_id
    
    # STATISTICS TESTS
    
    def test_get_stats(self, db, sample_recording_id, sample_segmentation_id, sample_effect_id):
        """Test getting comprehensive database statistics."""
        # Add some test data
        db.add_recording(sample_recording_id, "test.wav", "Test recording")
        db.add_segmentation(sample_segmentation_id, "manual", {}, "Test segmentation")
        
        # Add segment with embedding
        segment_id = f"seg_{uuid.uuid4().hex[:8]}"
        db.add_segment(
            segment_id=segment_id,
            source_id=sample_recording_id,
            segmentation_id=sample_segmentation_id,
            start=0.0,
            end=0.5,
            description="Test segment",
            embedding_text="test segment",
            faiss_index=123
        )
        
        # Add effect with preset
        db.add_effect(sample_effect_id, "Test Effect", "/test.maxpat")
        preset = {
            "parameters": [{"name": "param1", "value": 0.5}],
            "description": "Test preset",
            "embedding_text": "test preset",
            "FAISS_index": 456
        }
        db.add_preset_to_effect(sample_effect_id, preset)
        
        # Add performance
        performance_id = f"perf_{uuid.uuid4().hex[:8]}"
        db.add_performance(performance_id)
        
        # Get stats
        stats = db.get_stats()
        
        assert stats["recordings"] >= 1
        assert stats["segments"] >= 1
        assert stats["segments_with_embeddings"] >= 1
        assert stats["effects"] >= 1
        assert stats["presets"] >= 1
        assert stats["presets_with_embeddings"] >= 1
        assert stats["performances"] >= 1
        assert stats["segmentations"] >= 1
        assert stats["total_searchable_items"] >= 2  # segment + preset
    
    # INTEGRATION TESTS
    
    def test_full_workflow(self, db):
        """Test a complete workflow from recording to search."""
        # 1. Add a recording
        recording_id = f"rec_{uuid.uuid4().hex[:8]}"
        db.add_recording(
            recording_id=recording_id,
            path="forest/morning_birds.wav",
            description="Dawn chorus in temperate forest"
        )
        
        # 2. Add segmentation method
        segmentation_id = f"seg_method_{uuid.uuid4().hex[:8]}"
        db.add_segmentation(
            segmentation_id=segmentation_id,
            method="manual",
            parameters={"min_duration": 0.5},
            description="Hand-segmented bird calls"
        )
        
        # 3. Add segments
        segments = [
            {
                "id": f"seg_1_{uuid.uuid4().hex[:8]}",
                "start": 0.0, "end": 0.2,
                "description": "Robin morning call",
                "embedding_text": "robin bird call morning cheerful",
                "faiss_index": 100
            },
            {
                "id": f"seg_2_{uuid.uuid4().hex[:8]}",
                "start": 0.3, "end": 0.7,
                "description": "Blackbird territorial song",
                "embedding_text": "blackbird territorial song melodic",
                "faiss_index": 101
            }
        ]
        
        for seg in segments:
            db.add_segment(
                segment_id=seg["id"],
                source_id=recording_id,
                segmentation_id=segmentation_id,
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"],
                faiss_index=seg["faiss_index"]
            )
        
        # 4. Add an effect with presets
        effect_id = f"fx_{uuid.uuid4().hex[:8]}"
        db.add_effect(
            effect_id=effect_id,
            name="Bird Harmonizer",
            path="/effects/bird_harmonizer.maxpat",
            description="Adds harmonic layers to bird calls"
        )
        
        presets = [
            {
                "parameters": [
                    {"name": "harmony_count", "value": 3},
                    {"name": "pitch_shift", "value": 0.7},
                    {"name": "reverb", "value": 0.4}
                ],
                "description": "Ethereal bird chorus effect",
                "embedding_text": "ethereal bird chorus harmony atmospheric",
                "FAISS_index": 200
            },
            {
                "parameters": [
                    {"name": "harmony_count", "value": 1},
                    {"name": "pitch_shift", "value": -0.3},
                    {"name": "reverb", "value": 0.1}
                ],
                "description": "Deep forest ambience",
                "embedding_text": "deep forest ambience low pitch dark",
                "FAISS_index": 201
            }
        ]
        
        for preset in presets:
            db.add_preset_to_effect(effect_id, preset)
        
        # 5. Simulate a performance
        performance_id = f"perf_{uuid.uuid4().hex[:8]}"
        db.add_performance(performance_id)
        
        # Add some invocations
        db.add_invocation(performance_id, "cheerful morning robin", 0.0, segments[0]["id"])
        db.add_invocation(performance_id, "ethereal bird harmony", 5.2, effect=effect_id)
        
        # 6. Verify everything works
        # Check we can retrieve by FAISS IDs
        robin_segment = db.get_segment_by_faiss_id(100)
        assert robin_segment["description"] == "Robin morning call"
        
        ethereal_result = db.get_preset_by_faiss_id(200)
        assert ethereal_result is not None
        effect, preset = ethereal_result
        assert preset["description"] == "Ethereal bird chorus effect"
        
        # Check relationships work
        recording_segments = db.get_segments_by_recording(recording_id)
        assert len(recording_segments) == 2
        
        segmentation_segments = db.get_segments_by_segmentation(segmentation_id)
        assert len(segmentation_segments) == 2
        
        # Check performance
        performance = db.get_performance(performance_id)
        assert len(performance["invocations"]) == 2
        
        # Check stats
        stats = db.get_stats()
        assert stats["total_searchable_items"] >= 4  # 2 segments + 2 presets


# UTILITY TESTS AND FIXTURES

class TestDataFixtures:
    """Create realistic test data for manual testing."""
    
    @staticmethod
    def create_bird_recording_set(db: HibikidoDatabase):
        """Create a realistic set of bird recording data."""
        # Recording
        recording_id = "forest_dawn_001"
        db.add_recording(
            recording_id=recording_id,
            path="recordings/field/forest_dawn_001.wav",
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
        
        # Segments
        segments = [
            {
                "id": "robin_territorial_001",
                "start": 0.05, "end": 0.35,
                "description": "Robin territorial call, bright and assertive",
                "embedding_text": "robin territorial call bright assertive morning"
            },
            {
                "id": "blackbird_song_001", 
                "start": 0.45, "end": 0.95,
                "description": "Blackbird melodic song, complex phrases",
                "embedding_text": "blackbird melodic song complex phrases musical"
            },
            {
                "id": "wren_trill_001",
                "start": 1.2, "end": 1.6,
                "description": "Wren rapid trill, high frequency",
                "embedding_text": "wren rapid trill high frequency energetic"
            }
        ]
        
        for i, seg in enumerate(segments):
            db.add_segment(
                segment_id=seg["id"],
                source_id=recording_id,
                segmentation_id=segmentation_id,
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"],
                faiss_index=1000 + i
            )
        
        return recording_id, segmentation_id, [s["id"] for s in segments]
    
    @staticmethod
    def create_granular_effects(db: HibikidoDatabase):
        """Create realistic granular synthesis effects."""
        effect_id = "granular_processor_v2"
        db.add_effect(
            effect_id=effect_id,
            name="Granular Processor",
            path="/effects/granular/processor_v2.maxpat",
            description="Advanced granular synthesis with pitch and time manipulation"
        )
        
        presets = [
            {
                "parameters": [
                    {"name": "grain_size", "value": 0.05},
                    {"name": "pitch_variation", "value": 0.2},
                    {"name": "time_stretch", "value": 2.0},
                    {"name": "density", "value": 0.7}
                ],
                "description": "Ethereal time-stretched texture",
                "embedding_text": "ethereal time stretched texture atmospheric slow",
                "FAISS_index": 2000
            },
            {
                "parameters": [
                    {"name": "grain_size", "value": 0.01},
                    {"name": "pitch_variation", "value": 1.5},
                    {"name": "time_stretch", "value": 0.5},
                    {"name": "density", "value": 0.9}
                ],
                "description": "Glitchy fragmented rhythm",
                "embedding_text": "glitchy fragmented rhythm digital chaotic fast",
                "FAISS_index": 2001
            },
            {
                "parameters": [
                    {"name": "grain_size", "value": 0.1},
                    {"name": "pitch_variation", "value": 0.0},
                    {"name": "time_stretch", "value": 1.0},
                    {"name": "density", "value": 0.3}
                ],
                "description": "Sparse natural granulation",
                "embedding_text": "sparse natural granulation organic subtle",
                "FAISS_index": 2002
            }
        ]
        
        for preset in presets:
            db.add_preset_to_effect(effect_id, preset)
        
        return effect_id


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
        
        recording_id, segmentation_id, segment_ids = fixtures.create_bird_recording_set(db)
        effect_id = fixtures.create_granular_effects(db)
        
        # Test retrieval
        print(f"✅ Created recording: {recording_id}")
        print(f"✅ Created {len(segment_ids)} segments")
        print(f"✅ Created effect with presets: {effect_id}")
        
        # Test stats
        stats = db.get_stats()
        print(f"📊 Database stats: {stats}")
        
        # Test FAISS lookups
        robin_segment = db.get_segment_by_faiss_id(1000)
        if robin_segment:
            print(f"✅ Found segment by FAISS ID: {robin_segment['description']}")
        
        ethereal_result = db.get_preset_by_faiss_id(2000)
        if ethereal_result:
            effect, preset = ethereal_result
            print(f"✅ Found preset by FAISS ID: {preset['description']}")
        
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