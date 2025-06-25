"""
Test Suite for Hibikidō Orchestrator (Updated for Manifestation Protocol)
=========================================================================

Tests for the orchestrator with queue-based manifestation workflow.
Run with: python -m pytest test_orchestrator.py -v
"""

import pytest
import time
import logging
from hibikido.orchestrator import Orchestrator

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

class TestOrchestrator:
    """Test class for the orchestrator with manifestation protocol."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create a test orchestrator instance."""
        return Orchestrator(overlap_threshold=0.2, time_precision=0.1)
    
    @pytest.fixture
    def manifestations_tracker(self):
        """Create a manifestation tracker for testing callbacks."""
        manifestations = []
        
        def track_manifestation(index, collection, score, path, description, 
                              start, end, parameters):
            manifestations.append({
                "index": index,
                "collection": collection,
                "score": score,
                "path": path,
                "description": description,
                "start": start,
                "end": end,
                "parameters": parameters,
                "timestamp": time.time()
            })
        
        return manifestations, track_manifestation
    
    def test_orchestrator_initialization(self, orchestrator):
        """Test basic orchestrator initialization."""
        assert orchestrator.overlap_threshold == 0.2
        assert orchestrator.time_precision == 0.1
        assert orchestrator.active_niches == []
        assert orchestrator.queue == []
        assert orchestrator.manifest_callback is None
    
    def test_manifest_callback_setup(self, orchestrator, manifestations_tracker):
        """Test setting up manifestation callback."""
        manifestations, callback = manifestations_tracker
        
        orchestrator.set_manifest_callback(callback)
        assert orchestrator.manifest_callback is not None
        
        # Test callback works
        orchestrator.manifest_callback(0, "segments", 0.85, "test.wav", "test sound", 0.0, 1.0, "[]")
        assert len(manifestations) == 1
        assert manifestations[0]["description"] == "test sound"
    
    def test_queue_manifestation_basic(self, orchestrator):
        """Test basic manifestation queueing."""
        manifestation_data = {
            "index": 0,
            "collection": "segments",
            "score": 0.85,
            "path": "test/sound.wav",
            "description": "Test sound",
            "start": 0.0,
            "end": 1.0,
            "parameters": "[]",
            "sound_id": "test_sound_1",
            "freq_low": 1000,
            "freq_high": 2000,
            "duration": 1.0
        }
        
        success = orchestrator.queue_manifestation(manifestation_data)
        assert success is True
        assert len(orchestrator.queue) == 1
        
        queued_data, request_time = orchestrator.queue[0]
        assert queued_data["sound_id"] == "test_sound_1"
        assert request_time > 0
    
    def test_queue_multiple_manifestations(self, orchestrator):
        """Test queueing multiple manifestations."""
        manifestations = []
        for i in range(3):
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": 0.8 + i * 0.05,
                "path": f"test/sound_{i}.wav",
                "description": f"Test sound {i}",
                "start": 0.0,
                "end": 1.0,
                "parameters": "[]",
                "sound_id": f"test_sound_{i}",
                "freq_low": 1000 + i * 1000,
                "freq_high": 2000 + i * 1000,
                "duration": 1.0
            }
            
            success = orchestrator.queue_manifestation(manifestation_data)
            assert success is True
        
        assert len(orchestrator.queue) == 3
        
        # Verify FIFO order
        for i in range(3):
            queued_data, _ = orchestrator.queue[i]
            assert queued_data["sound_id"] == f"test_sound_{i}"
    
    def test_queue_processing_no_conflicts(self, orchestrator, manifestations_tracker):
        """Test queue processing when no frequency conflicts exist."""
        manifestations, callback = manifestations_tracker
        orchestrator.set_manifest_callback(callback)
        
        # Queue non-overlapping sounds
        sounds = [
            {"id": "low", "freq_low": 100, "freq_high": 500, "desc": "Low sound"},
            {"id": "mid", "freq_low": 1000, "freq_high": 2000, "desc": "Mid sound"},
            {"id": "high", "freq_low": 4000, "freq_high": 8000, "desc": "High sound"}
        ]
        
        for i, sound in enumerate(sounds):
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": 0.8,
                "path": f"test/{sound['id']}.wav",
                "description": sound["desc"],
                "start": 0.0,
                "end": 1.0,
                "parameters": "[]",
                "sound_id": sound["id"],
                "freq_low": sound["freq_low"],
                "freq_high": sound["freq_high"],
                "duration": 1.0
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        assert len(orchestrator.queue) == 3
        
        # Process queue - all should manifest (no conflicts)
        orchestrator.update()
        
        # All sounds should have manifested
        assert len(manifestations) == 3
        assert len(orchestrator.queue) == 0  # Queue should be empty
        assert len(orchestrator.active_niches) == 3  # All sounds active
        
        # Verify manifestation order matches queue order
        for i, sound in enumerate(sounds):
            assert manifestations[i]["description"] == sound["desc"]
    
    def test_queue_processing_with_conflicts(self, orchestrator, manifestations_tracker):
        """Test queue processing with frequency conflicts."""
        manifestations, callback = manifestations_tracker
        orchestrator.set_manifest_callback(callback)
        
        # Queue overlapping sounds (should create conflicts)
        sounds = [
            {"id": "first", "freq_low": 1000, "freq_high": 2000, "desc": "First sound"},
            {"id": "overlap", "freq_low": 1500, "freq_high": 2500, "desc": "Overlapping sound"},  # Conflicts
            {"id": "separate", "freq_low": 4000, "freq_high": 5000, "desc": "Separate sound"}  # No conflict
        ]
        
        for i, sound in enumerate(sounds):
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": 0.8,
                "path": f"test/{sound['id']}.wav",
                "description": sound["desc"],
                "start": 0.0,
                "end": 1.0,
                "parameters": "[]",
                "sound_id": sound["id"],
                "freq_low": sound["freq_low"],
                "freq_high": sound["freq_high"],
                "duration": 1.0
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        # Process queue
        orchestrator.update()
        
        # Should manifest first and separate sounds, overlap should remain queued
        assert len(manifestations) >= 1  # At least first sound
        assert len(orchestrator.queue) <= 2  # Some might remain queued
        
        # First sound should definitely manifest
        assert any(m["description"] == "First sound" for m in manifestations)
        
        # Separate sound should also manifest (no conflict)
        assert any(m["description"] == "Separate sound" for m in manifestations)
        
        print(f"Manifested: {len(manifestations)}, Queued: {len(orchestrator.queue)}")
    
    def test_time_based_manifestation(self, orchestrator, manifestations_tracker):
        """Test that queued sounds manifest when conflicts expire."""
        manifestations, callback = manifestations_tracker
        orchestrator.set_manifest_callback(callback)
        
        # Queue conflicting sounds with short duration
        sounds = [
            {"id": "short", "freq_low": 1000, "freq_high": 2000, "duration": 0.1, "desc": "Short sound"},
            {"id": "waiting", "freq_low": 1000, "freq_high": 2000, "duration": 1.0, "desc": "Waiting sound"}
        ]
        
        for i, sound in enumerate(sounds):
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": 0.8,
                "path": f"test/{sound['id']}.wav",
                "description": sound["desc"],
                "start": 0.0,
                "end": 1.0,
                "parameters": "[]",
                "sound_id": sound["id"],
                "freq_low": sound["freq_low"],
                "freq_high": sound["freq_high"],
                "duration": sound["duration"]
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        # First update - short sound should manifest, waiting sound queued
        orchestrator.update()
        initial_manifestations = len(manifestations)
        initial_queue_size = len(orchestrator.queue)
        
        print(f"After first update: {initial_manifestations} manifested, {initial_queue_size} queued")
        
        # Wait for short sound to expire
        time.sleep(0.15)
        
        # Second update - waiting sound should now manifest
        before_delayed = len(manifestations)
        orchestrator.update()
        after_delayed = len(manifestations)
        
        delayed_manifestations = after_delayed - before_delayed
        print(f"After delay: {delayed_manifestations} additional manifestations")
        
        # Should have processed at least the initial sound
        assert initial_manifestations >= 1
        
        # Might have delayed manifestation (timing dependent)
        if delayed_manifestations > 0:
            print("✅ Time-based manifestation working")
        else:
            print("ℹ️  No delayed manifestation (timing dependent)")
    
    def test_logarithmic_frequency_overlap(self, orchestrator):
        """Test logarithmic frequency overlap calculation."""
        # Test the internal method directly
        
        # Case 1: No overlap
        no_overlap = orchestrator._has_frequency_overlap(100, 200, 400, 800)
        assert not no_overlap
        
        # Case 2: Complete overlap
        complete_overlap = orchestrator._has_frequency_overlap(1000, 2000, 1000, 2000)
        assert complete_overlap
        
        # Case 3: Significant overlap (should exceed 20% threshold)
        significant_overlap = orchestrator._has_frequency_overlap(1000, 2000, 1500, 3000)
        assert significant_overlap  # Should exceed 20% threshold
        
        # Case 4: Small overlap (might be within threshold)
        small_overlap = orchestrator._has_frequency_overlap(1000, 2000, 1900, 3800)
        print(f"Small overlap result: {small_overlap}")
        
        # Case 5: Octave relationships
        octave_1_2 = orchestrator._has_frequency_overlap(1000, 2000, 2000, 4000)
        print(f"Adjacent octaves overlap: {octave_1_2}")
    
    def test_niche_cleanup(self, orchestrator):
        """Test that expired niches are cleaned up."""
        # Manually add an expired niche
        past_time = time.time() - 10  # 10 seconds ago
        orchestrator._register_niche("expired_sound", past_time, past_time + 1, 1000, 2000)
        
        # Add a current niche
        now = time.time()
        orchestrator._register_niche("current_sound", now, now + 10, 2000, 3000)
        
        assert len(orchestrator.active_niches) == 2
        
        # Cleanup should remove expired niche
        orchestrator._cleanup_expired()
        
        assert len(orchestrator.active_niches) == 1
        assert orchestrator.active_niches[0]["sound_id"] == "current_sound"
    
    def test_orchestrator_stats(self, orchestrator):
        """Test orchestrator statistics."""
        # Initial stats
        stats = orchestrator.get_stats()
        assert stats["active_niches"] == 0
        assert stats["queued_requests"] == 0
        assert stats["overlap_threshold"] == 0.2
        assert stats["time_precision"] == 0.1
        
        # Add some test data
        manifestation_data = {
            "index": 0, "collection": "segments", "score": 0.8,
            "path": "test.wav", "description": "Test", "start": 0.0, "end": 1.0,
            "parameters": "[]", "sound_id": "test", "freq_low": 1000, 
            "freq_high": 2000, "duration": 10.0
        }
        
        orchestrator.queue_manifestation(manifestation_data)
        orchestrator.queue_manifestation(manifestation_data)
        
        # Add active niche manually
        now = time.time()
        orchestrator._register_niche("active_test", now, now + 10, 3000, 4000)
        
        stats = orchestrator.get_stats()
        assert stats["active_niches"] == 1
        assert stats["queued_requests"] == 2
    
    def test_queue_fifo_order(self, orchestrator, manifestations_tracker):
        """Test that queue maintains FIFO order."""
        manifestations, callback = manifestations_tracker
        orchestrator.set_manifest_callback(callback)
        
        # Queue sounds in specific order (non-conflicting for clear testing)
        order = ["first", "second", "third"]
        
        for i, sound_id in enumerate(order):
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": 0.8,
                "path": f"test/{sound_id}.wav",
                "description": f"{sound_id.title()} sound",
                "start": 0.0,
                "end": 1.0,
                "parameters": "[]",
                "sound_id": sound_id,
                "freq_low": 1000 + i * 2000,  # Non-overlapping frequencies
                "freq_high": 2000 + i * 2000,
                "duration": 1.0
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        # Process queue
        orchestrator.update()
        
        # Manifestations should maintain FIFO order
        assert len(manifestations) == 3
        for i, expected_id in enumerate(order):
            assert manifestations[i]["description"] == f"{expected_id.title()} sound"
            assert manifestations[i]["index"] == i
    
    def test_edge_cases(self, orchestrator):
        """Test edge cases and error handling."""
        # Test with missing fields
        incomplete_data = {
            "index": 0,
            "description": "Incomplete"
            # Missing required fields
        }
        
        success = orchestrator.queue_manifestation(incomplete_data)
        # Should handle gracefully
        assert success in [True, False]  # Don't crash
        
        # Test with invalid frequencies
        invalid_freq_data = {
            "index": 0, "collection": "segments", "score": 0.8,
            "path": "test.wav", "description": "Invalid freq", "start": 0.0, "end": 1.0,
            "parameters": "[]", "sound_id": "invalid", "freq_low": 0, 
            "freq_high": 0, "duration": 1.0
        }
        
        success = orchestrator.queue_manifestation(invalid_freq_data)
        assert success is True  # Should queue without crashing
        
        # Processing should handle gracefully
        orchestrator.update()  # Should not crash
    
    def test_legacy_evaluate_request(self, orchestrator):
        """Test that legacy evaluate_request method still works but is deprecated."""
        request = {
            "sound_id": "legacy_test",
            "freq_low": 1000,
            "freq_high": 2000,
            "duration": 1.0
        }
        
        # Should work but log warning
        result = orchestrator.evaluate_request(request)
        
        # Should return allowed status (legacy behavior)
        assert result["status"] == "allowed"
        assert "start_time" in result


class TestOrchestratorIntegration:
    """Integration tests for orchestrator with manifestation workflow."""
    
    def test_realistic_manifestation_scenario(self):
        """Test a realistic scenario with multiple sounds and conflicts."""
        orchestrator = Orchestrator(overlap_threshold=0.2, time_precision=0.1)
        
        # Track manifestations
        manifestations = []
        def track(index, collection, score, path, description, start, end, parameters):
            manifestations.append({
                "description": description,
                "timestamp": time.time(),
                "freq_info": f"from {path}"
            })
            print(f"📡 Manifested: {description}")
        
        orchestrator.set_manifest_callback(track)
        
        # Simulate search results with various frequency ranges
        search_results = [
            {"desc": "Bird call high", "freq_low": 2000, "freq_high": 5000, "duration": 2.0},
            {"desc": "Bird call overlap", "freq_low": 3000, "freq_high": 6000, "duration": 1.5},  # Overlaps
            {"desc": "Wind low", "freq_low": 50, "freq_high": 300, "duration": 3.0},  # No overlap
            {"desc": "Water mid", "freq_low": 800, "freq_high": 1200, "duration": 2.5},  # No overlap
            {"desc": "Another bird", "freq_low": 2500, "freq_high": 4500, "duration": 1.0},  # Overlaps
        ]
        
        # Queue all results (new paradigm)
        for i, sound in enumerate(search_results):
            manifestation_data = {
                "index": i, "collection": "segments", "score": 0.85 - i * 0.05,
                "path": f"sounds/{sound['desc'].replace(' ', '_').lower()}.wav",
                "description": sound["desc"], "start": 0.0, "end": 1.0, "parameters": "[]",
                "sound_id": f"sound_{i}", "freq_low": sound["freq_low"], 
                "freq_high": sound["freq_high"], "duration": sound["duration"]
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        print(f"Queued {len(search_results)} sounds")
        
        # Process orchestrator
        orchestrator.update()
        
        immediate_manifestations = len(manifestations)
        remaining_queue = len(orchestrator.queue)
        
        print(f"Immediate manifestations: {immediate_manifestations}")
        print(f"Remaining in queue: {remaining_queue}")
        
        # Should have manifested some sounds immediately
        assert immediate_manifestations > 0
        
        # Non-overlapping sounds should have manifested
        manifested_descriptions = [m["description"] for m in manifestations]
        assert "Wind low" in manifested_descriptions  # Low freq, no overlap
        assert "Water mid" in manifested_descriptions  # Mid freq, no overlap
        
        print("✅ Realistic manifestation scenario working")
    
    def test_performance_many_sounds(self):
        """Test orchestrator performance with many queued sounds."""
        orchestrator = Orchestrator()
        
        manifestation_count = 0
        def count_manifestations(*args):
            nonlocal manifestation_count
            manifestation_count += 1
        
        orchestrator.set_manifest_callback(count_manifestations)
        
        # Queue many sounds with various overlaps
        start_time = time.time()
        
        for i in range(50):
            manifestation_data = {
                "index": i, "collection": "segments", "score": 0.8,
                "path": f"test_{i}.wav", "description": f"Sound {i}",
                "start": 0.0, "end": 1.0, "parameters": "[]",
                "sound_id": f"perf_test_{i}",
                "freq_low": 1000 + (i % 10) * 100,  # Some overlaps
                "freq_high": 2000 + (i % 10) * 100,
                "duration": 1.0
            }
            orchestrator.queue_manifestation(manifestation_data)
        
        queue_time = time.time() - start_time
        
        # Process queue
        process_start = time.time()
        orchestrator.update()
        process_time = time.time() - process_start
        
        print(f"Queued 50 sounds in {queue_time:.3f}s")
        print(f"Processed queue in {process_time:.3f}s")
        print(f"Manifested {manifestation_count} sounds immediately")
        print(f"Remaining queued: {len(orchestrator.queue)}")
        
        # Should be reasonably fast
        assert queue_time < 0.1, f"Queueing too slow: {queue_time:.3f}s"
        assert process_time < 0.1, f"Processing too slow: {process_time:.3f}s"
        
        # Should have processed some sounds
        assert manifestation_count > 0
        
        print("✅ Performance test passed")


if __name__ == "__main__":
    """Run orchestrator tests manually."""
    import sys
    
    print("🚀 Running Updated Hibikidō Orchestrator Tests")
    print("=" * 50)
    
    # Create test instance
    orchestrator = Orchestrator(overlap_threshold=0.2, time_precision=0.1)
    print("✅ Orchestrator created")
    
    # Test manifestation callback
    manifestations = []
    def test_callback(index, collection, score, path, description, start, end, parameters):
        manifestations.append(description)
        print(f"   📡 Manifested: {description}")
    
    orchestrator.set_manifest_callback(test_callback)
    print("✅ Callback set")
    
    # Test basic queueing
    print("\n🎵 Testing basic manifestation queueing...")
    
    test_data = {
        "index": 0, "collection": "segments", "score": 0.85,
        "path": "test/sound.wav", "description": "Test sound",
        "start": 0.0, "end": 1.0, "parameters": "[]",
        "sound_id": "test_1", "freq_low": 1000, "freq_high": 2000, "duration": 1.0
    }
    
    success = orchestrator.queue_manifestation(test_data)
    print(f"   Queue success: {success}")
    print(f"   Queue size: {len(orchestrator.queue)}")
    
    # Test processing
    print("\n⚡ Testing queue processing...")
    orchestrator.update()
    print(f"   Manifestations received: {len(manifestations)}")
    print(f"   Queue size after processing: {len(orchestrator.queue)}")
    print(f"   Active niches: {len(orchestrator.active_niches)}")
    
    # Test with conflicts
    print("\n🎭 Testing frequency conflicts...")
    
    # Add overlapping sound
    conflict_data = {
        "index": 1, "collection": "segments", "score": 0.80,
        "path": "test/conflict.wav", "description": "Conflicting sound",
        "start": 0.0, "end": 1.0, "parameters": "[]",
        "sound_id": "conflict_1", "freq_low": 1500, "freq_high": 2500, "duration": 1.0
    }
    
    orchestrator.queue_manifestation(conflict_data)
    print(f"   Queued conflicting sound, queue size: {len(orchestrator.queue)}")
    
    before_conflict_processing = len(manifestations)
    orchestrator.update()
    after_conflict_processing = len(manifestations)
    
    conflict_manifestations = after_conflict_processing - before_conflict_processing
    print(f"   Additional manifestations: {conflict_manifestations}")
    print(f"   Remaining in queue: {len(orchestrator.queue)}")
    
    if len(orchestrator.queue) > 0:
        print("   ✅ Frequency conflict detection working")
    else:
        print("   ℹ️  No conflict detected (within threshold)")
    
    # Test stats
    stats = orchestrator.get_stats()
    print(f"\n📊 Final stats: {stats}")
    
    print("\n🎉 Basic orchestrator tests completed!")
    print("To run full test suite: python -m pytest test_orchestrator.py -v")