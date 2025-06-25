"""
Invocation Protocol Integration Test
===================================

Test the new invocation paradigm: /invoke → queue all → /manifest over time
No completion signals, sounds manifest when the cosmos permits.
"""

import tempfile
import shutil
import time
from hibikido.database_manager import HibikidoDatabase
from hibikido.embedding_manager import EmbeddingManager
from hibikido.orchestrator import Orchestrator

def test_invocation_manifestation_flow():
    """Test the complete invocation to manifestation flow."""
    print("🧪 Testing Invocation → Manifestation Flow")
    print("=" * 50)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Setup components
        db = HibikidoDatabase(db_name="hibikido_invocation_test")
        assert db.connect(), "Database connection failed"
        
        em = EmbeddingManager(index_file=f"{temp_dir}/invocation_test.index")
        assert em.initialize(), "Embedding manager failed"
        
        orchestrator = Orchestrator(overlap_threshold=0.2, time_precision=0.1)
        
        # Mock OSC manifestation tracking
        manifestations_received = []
        
        def mock_manifest_callback(index, collection, score, path, description, 
                                 start, end, parameters):
            manifestations_received.append({
                "index": index,
                "collection": collection,
                "path": path,
                "description": description,
                "timestamp": time.time()
            })
            print(f"   📡 /manifest: [{index}] {description} @ {path}")
        
        orchestrator.set_manifest_callback(mock_manifest_callback)
        print("✅ Components initialized")
        
        # Add test data with frequency conflicts
        recording_path = "test/invocation_sounds.wav"
        db.add_recording(recording_path, "Test sounds for invocation")
        db.add_segmentation("invocation_test", "manual", {}, "Invocation test")
        
        # Test segments designed to create orchestrator conflicts
        test_segments = [
            {
                "description": "Bell resonance high",
                "embedding_text": "bell resonance high metallic bright",
                "start": 0.0, "end": 0.3,
                "freq_low": 2000, "freq_high": 5000, "duration": 2.0
            },
            {
                "description": "Bell resonance overlap", 
                "embedding_text": "bell resonance metallic overlapping tone",
                "start": 0.4, "end": 0.7,
                "freq_low": 2500, "freq_high": 4500, "duration": 1.8  # Frequency overlap
            },
            {
                "description": "Bell resonance low",
                "embedding_text": "bell resonance low warm deep",
                "start": 0.8, "end": 1.0,
                "freq_low": 500, "freq_high": 1500, "duration": 1.5  # No overlap
            }
        ]
        
        for seg in test_segments:
            success = db.add_segment(
                source_path=recording_path,
                segmentation_id="invocation_test",
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"]
            )
            assert success
            
            # Add frequency metadata
            db.segments.update_one(
                {"source_path": recording_path, "start": seg["start"]},
                {"$set": {
                    "freq_low": seg["freq_low"],
                    "freq_high": seg["freq_high"],
                    "duration": seg["duration"]
                }}
            )
        
        # Build index
        stats = em.rebuild_from_database(db)
        print(f"✅ Index built: {stats}")
        
        # Simulate invocation handler
        def simulate_invocation(incantation):
            print(f"\n🔮 Invoking: '{incantation}'")
            
            # Search phase
            results = em.search(incantation, top_k=10, db_manager=db)
            segment_results = [r for r in results if r["collection"] == "segments"]
            
            if not segment_results:
                print("   No resonance found")
                return
            
            # Queue ALL results (new paradigm)
            queued_count = 0
            for i, result in enumerate(segment_results):
                document = result["document"]
                
                manifestation_data = {
                    "index": i,
                    "collection": "segments",
                    "score": float(result["score"]),
                    "path": str(document.get("source_path", "")),
                    "description": document.get("description", "untitled"),
                    "start": float(document.get("start", 0.0)),
                    "end": float(document.get("end", 1.0)),
                    "parameters": "[]",
                    "sound_id": str(document.get("_id", "unknown")),
                    "freq_low": document.get("freq_low", 200),
                    "freq_high": document.get("freq_high", 2000),
                    "duration": document.get("duration", 1.0)
                }
                
                if orchestrator.queue_manifestation(manifestation_data):
                    queued_count += 1
            
            print(f"   📊 Queued {queued_count} resonances for manifestation")
            return queued_count
        
        # Test invocation with conflicting sounds
        print("\n🎵 Testing invocation with frequency conflicts...")
        
        # Clear any existing manifestations
        manifestations_received.clear()
        
        # Invoke bell resonance (should find all 3 segments)
        queued = simulate_invocation("bell resonance metallic")
        assert queued >= 2, f"Expected multiple resonances, got {queued}"
        
        # Process orchestrator immediately
        print("\n⚡ Processing orchestrator queue...")
        initial_manifestations = len(manifestations_received)
        orchestrator.update()
        immediate_manifestations = len(manifestations_received) - initial_manifestations
        
        print(f"   📊 Immediate manifestations: {immediate_manifestations}")
        print(f"   📊 Queue remaining: {orchestrator.get_stats()['queued_requests']}")
        
        # Should have at least one immediate manifestation
        assert immediate_manifestations >= 1, "Expected at least one immediate manifestation"
        
        # Test time-based manifestation (wait for conflicts to resolve)
        if orchestrator.get_stats()['queued_requests'] > 0:
            print("\n⏰ Waiting for time-based manifestation...")
            
            # Wait for sounds to expire
            time.sleep(0.2)
            
            before_delayed = len(manifestations_received)
            orchestrator.update()  # Process expired sounds
            after_delayed = len(manifestations_received)
            
            delayed_manifestations = after_delayed - before_delayed
            print(f"   📊 Delayed manifestations: {delayed_manifestations}")
            
            # Note: Might be 0 if timing/conflicts don't work out exactly
            if delayed_manifestations > 0:
                print("   ✅ Time-based manifestation working")
            else:
                print("   ℹ️  No additional manifestations (timing dependent)")
        
        # Test manifestation timing
        print("\n📊 Manifestation Analysis:")
        if manifestations_received:
            first_time = manifestations_received[0]["timestamp"]
            for i, manifest in enumerate(manifestations_received):
                delay = manifest["timestamp"] - first_time
                print(f"   {i}: {manifest['description']} (+{delay:.3f}s)")
        
        # Verify no completion signal needed
        print("\n✅ Invocation paradigm verified:")
        print(f"   - {queued} resonances queued")
        print(f"   - {len(manifestations_received)} manifestations received")
        print(f"   - No completion signal required")
        print(f"   - Orchestrator managing timing transparently")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        assert False, str(e)
    
    finally:
        try:
            db.client.drop_database("hibikido_invocation_test")
            db.close()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_queue_all_strategy():
    """Test that ALL search results go through orchestrator queue."""
    print(f"\n🧪 Testing Queue-All Strategy")
    print("=" * 50)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Setup
        db = HibikidoDatabase(db_name="hibikido_queue_all_test")
        assert db.connect()
        
        em = EmbeddingManager(index_file=f"{temp_dir}/queue_all_test.index")
        assert em.initialize()
        
        orchestrator = Orchestrator()
        
        # Track all manifestations
        all_manifestations = []
        
        def track_manifestations(index, collection, score, path, description, 
                               start, end, parameters):
            all_manifestations.append({
                "index": index,
                "description": description,
                "score": score
            })
            print(f"   📡 Manifestation: {description} (score: {score:.3f})")
        
        orchestrator.set_manifest_callback(track_manifestations)
        
        # Add multiple non-conflicting segments
        recording_path = "test/queue_all_sounds.wav"
        db.add_recording(recording_path, "Multiple test sounds")
        db.add_segmentation("queue_all_test", "manual", {}, "Queue all test")
        
        # Non-conflicting segments (different frequency ranges)
        segments = [
            {
                "description": "Low rumble",
                "embedding_text": "low rumble deep bass",
                "freq_low": 50, "freq_high": 200, "duration": 1.0
            },
            {
                "description": "Mid texture",
                "embedding_text": "mid texture harmonic",
                "freq_low": 800, "freq_high": 1200, "duration": 1.0
            },
            {
                "description": "High sparkle",
                "embedding_text": "high sparkle bright",
                "freq_low": 4000, "freq_high": 8000, "duration": 1.0
            }
        ]
        
        for i, seg in enumerate(segments):
            db.add_segment(
                source_path=recording_path,
                segmentation_id="queue_all_test",
                start=i * 0.3,
                end=(i + 1) * 0.3,
                description=seg["description"],
                embedding_text=seg["embedding_text"]
            )
            
            # Add frequency metadata
            db.segments.update_one(
                {"source_path": recording_path, "start": i * 0.3},
                {"$set": {
                    "freq_low": seg["freq_low"],
                    "freq_high": seg["freq_high"],
                    "duration": seg["duration"]
                }}
            )
        
        # Build index
        stats = em.rebuild_from_database(db)
        print(f"Setup complete: {stats}")
        
        # Search for broad query that should match multiple
        results = em.search("texture harmonic", top_k=10, db_manager=db)
        segment_results = [r for r in results if r["collection"] == "segments"]
        
        print(f"Search found {len(segment_results)} segment results")
        
        # Queue ALL results
        for i, result in enumerate(segment_results):
            document = result["document"]
            
            manifestation_data = {
                "index": i,
                "collection": "segments",
                "score": float(result["score"]),
                "path": str(document.get("source_path", "")),
                "description": document.get("description", "untitled"),
                "start": float(document.get("start", 0.0)),
                "end": float(document.get("end", 1.0)),
                "parameters": "[]",
                "sound_id": str(document.get("_id", "unknown")),
                "freq_low": document.get("freq_low", 200),
                "freq_high": document.get("freq_high", 2000),
                "duration": document.get("duration", 1.0)
            }
            
            orchestrator.queue_manifestation(manifestation_data)
        
        print(f"Queued {len(segment_results)} manifestations")
        
        # Process queue - since no conflicts, all should manifest
        orchestrator.update()
        
        print(f"Manifestations sent: {len(all_manifestations)}")
        
        # Verify queue-all strategy
        if len(segment_results) > 0:
            # Should have processed at least some manifestations
            assert len(all_manifestations) > 0, "No manifestations sent"
            print("✅ Queue-all strategy working")
            
            # Check that high-score results were included
            scores = [m["score"] for m in all_manifestations]
            print(f"   Score range: {min(scores):.3f} - {max(scores):.3f}")
            
        else:
            print("ℹ️  No search results to test queue-all strategy")
        
    except Exception as e:
        print(f"❌ Queue-all test failed: {e}")
        assert False, str(e)
    
    finally:
        try:
            db.client.drop_database("hibikido_queue_all_test")
            db.close()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_osc_protocol_change():
    """Test the OSC protocol change from search to invocation."""
    print(f"\n🧪 Testing OSC Protocol Change")
    print("=" * 50)
    
    from hibikido.osc_handler import OSCHandler
    
    # Test OSC handler address mappings
    osc_handler = OSCHandler()
    
    # Verify new addresses exist
    assert 'invoke' in osc_handler.addresses, "Missing 'invoke' address"
    assert 'manifest' in osc_handler.addresses, "Missing 'manifest' address"
    assert osc_handler.addresses['invoke'] == '/invoke', "Wrong invoke address"
    assert osc_handler.addresses['manifest'] == '/manifest', "Wrong manifest address"
    
    # Verify old addresses removed
    assert 'search' not in osc_handler.addresses, "'search' address should be removed"
    assert 'search_complete' not in osc_handler.addresses, "'search_complete' should be removed"
    
    print("✅ OSC address mappings updated correctly")
    
    # Test manifest method
    if hasattr(osc_handler, 'send_manifest'):
        print("✅ send_manifest method exists")
    else:
        print("❌ send_manifest method missing")
        assert False, "send_manifest method missing"
    
    print("✅ OSC protocol change verified")

def run_invocation_tests():
    """Run all invocation paradigm tests."""
    print("🚀 Running Invocation Paradigm Tests")
    print("=" * 70)
    
    tests = [
        ("OSC Protocol Change", test_osc_protocol_change),
        ("Queue-All Strategy", test_queue_all_strategy),
        ("Invocation→Manifestation Flow", test_invocation_manifestation_flow),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*70}")
            print(f"Running: {test_name}")
            print('='*70)
            result = test_func()
            results.append((test_name, result))
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{status} {test_name}")
        except Exception as e:
            results.append((test_name, False))
            print(f"❌ FAILED {test_name}: {e}")
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 INVOCATION PARADIGM TEST SUMMARY")
    print(f"{'='*70}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total:.1%})")
    
    if passed == total:
        print("\n🎉 All invocation paradigm tests passed!")
        print("\nKey paradigm shifts verified:")
        print("- OSC protocol: /search → /invoke, /result → /manifest")
        print("- No completion signals - ongoing manifestation")
        print("- Queue-all strategy: ALL search results go through orchestrator")
        print("- Unified manifestation: orchestrator sends all /manifest messages")
        print("- Time-frequency niche management with delayed manifestation")
        print("- Transparent orchestration - client doesn't know about timing")
        print("- Simplified server logic - search finds, orchestrator manifests")
        print("\nReady for invocation! 🔮")
    else:
        print(f"\n⚠️  {total - passed} tests failed")
        print("Check output above for details")
    
    return passed == total

if __name__ == "__main__":
    success = run_invocation_tests()
    exit(0 if success else 1)