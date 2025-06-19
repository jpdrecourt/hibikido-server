"""
Simple Integration Test
=======================

Test the simplified search system: OSC -> FAISS -> MongoDB -> OSC
"""

import tempfile
import shutil
from hibikido.database_manager import HibikidoDatabase
from hibikido.embedding_manager import EmbeddingManager

def test_simple_search_flow():
    """Test the complete simple search flow."""
    print("🧪 Testing Simple Search Flow")
    print("=" * 40)
    
    # Setup
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Initialize database
        db = HibikidoDatabase(db_name="hibikido_simple_test")
        if not db.connect():
            print("❌ Failed to connect to MongoDB")
            return False
        
        # Initialize embedding manager
        index_file = f"{temp_dir}/simple_test.index"
        
        em = EmbeddingManager(index_file=index_file)
        
        if not em.initialize():
            print("❌ Failed to initialize embedding manager")
            return False
        
        print("✅ Database and embedding manager initialized")
        
        # Add test data
        print("\n📝 Adding test data...")
        
        # Add recording
        db.add_recording("forest_001", "test/forest.wav", "Morning forest with birds")
        
        # Add segmentation
        db.add_segmentation("manual_v1", "manual", {}, "Manual segmentation")
        
        # Add segments
        segments = [
            {
                "id": "robin_call", 
                "embedding_text": "bright robin territorial call morning"
            },
            {
                "id": "blackbird_song",
                "embedding_text": "melodic blackbird complex musical phrase"
            },
            {
                "id": "forest_ambience", 
                "embedding_text": "peaceful forest atmosphere ambient natural"
            }
        ]
        
        for seg in segments:
            db.add_segment(
                segment_id=seg["id"],
                source_id="forest_001",
                segmentation_id="manual_v1", 
                start=0.0, end=1.0,
                description=f"Test segment: {seg['id']}",
                embedding_text=seg["embedding_text"]
            )
        
        # Add effect
        db.add_effect("reverb_001", "Reverb Effect", "/effects/reverb.maxpat", "Reverb processor")
        
        # Add presets
        presets = [
            {
                "parameters": [{"name": "room_size", "value": 0.8}],
                "description": "Cathedral reverb",
                "embedding_text": "cathedral spacious reverb long decay sacred"
            },
            {
                "parameters": [{"name": "room_size", "value": 0.2}],
                "description": "Intimate room",
                "embedding_text": "intimate small room reverb close warm"
            }
        ]
        
        for preset in presets:
            db.add_preset_to_effect("reverb_001", preset)
        
        print(f"✅ Added {len(segments)} segments and {len(presets)} presets")
        
        # Rebuild FAISS index from database
        print("\n🔄 Building FAISS index...")
        stats = em.rebuild_from_database(db)
        print(f"📊 Build stats: {stats}")
        
        if em.get_total_embeddings() != 5:  # 3 segments + 2 presets
            print(f"❌ Expected 5 embeddings, got {em.get_total_embeddings()}")
            return False
        
        print("✅ FAISS index built successfully")
        
        # Test searches
        print("\n🔍 Testing searches...")
        
        test_queries = [
            {
                "query": "bright bird call",
                "expected_collections": ["segments"],
                "expected_contains": ["robin"]
            },
            {
                "query": "melodic musical phrase", 
                "expected_collections": ["segments"],
                "expected_contains": ["blackbird"]
            },
            {
                "query": "cathedral spacious reverb",
                "expected_collections": ["presets"],
                "expected_contains": ["cathedral"]
            },
            {
                "query": "peaceful forest atmosphere",
                "expected_collections": ["segments"],
                "expected_contains": ["forest"]
            }
        ]
        
        all_passed = True
        
        for i, test in enumerate(test_queries):
            print(f"\n  Test {i+1}: '{test['query']}'")
            
            results = em.search(test["query"], top_k=3, db_manager=db)
            
            if not results:
                print("    ❌ No results found")
                all_passed = False
                continue
            
            print(f"    📋 Found {len(results)} results:")
            for j, result in enumerate(results):
                collection = result["collection"]
                score = result["score"]
                doc = result["document"]
                
                if collection == "segments":
                    desc = doc.get("description", "no description")
                elif collection == "presets":
                    desc = doc.get("description", "no description")
                else:
                    desc = "unknown document type"
                
                print(f"       {j+1}. [{collection}] {desc} (score: {score:.3f})")
            
            # Check if expected results are in top results
            top_collections = [r["collection"] for r in results[:2]]
            all_text = " ".join([str(r["document"]) for r in results[:2]]).lower()
            
            collection_match = any(c in top_collections for c in test["expected_collections"])
            content_match = any(content.lower() in all_text for content in test["expected_contains"])
            
            if collection_match and content_match:
                print("    ✅ Expected results found")
            else:
                print(f"    ❌ Expected not found - collections: {collection_match}, content: {content_match}")
                all_passed = False
        
        # Test MongoDB integration
        print(f"\n🗄️  Testing MongoDB integration...")
        
        # Test segment has FAISS_index
        segment_doc = db.get_segment("robin_call") 
        if segment_doc and "FAISS_index" in segment_doc:
            print("✅ Segment has FAISS_index")
        else:
            print("❌ Segment missing FAISS_index")
            all_passed = False
        
        # Test preset has FAISS_index
        effect_doc = db.get_effect("reverb_001")
        if (effect_doc and len(effect_doc.get("presets", [])) >= 1 and 
            "FAISS_index" in effect_doc["presets"][0]):
            print("✅ Preset has FAISS_index")
        else:
            print("❌ Preset missing FAISS_index")
            all_passed = False
        
        # Test persistence
        print(f"\n💾 Testing persistence...")
        
        # Create new embedding manager to test loading
        em2 = EmbeddingManager(index_file=index_file)
        
        if em2.initialize() and em2.get_total_embeddings() == 5:
            print("✅ Index loaded successfully")
        else:
            print("❌ Failed to load index")
            all_passed = False
        
        # Test search on loaded index
        loaded_results = em2.search("bright bird", top_k=3, db_manager=db)
        if loaded_results and loaded_results[0]["document"]["_id"] == "robin_call":
            print("✅ Search works on loaded index")
        else:
            print("❌ Search failed on loaded index")
            all_passed = False
        
        # Summary
        print(f"\n{'='*40}")
        if all_passed:
            print("🎉 All simple search tests PASSED!")
        else:
            print("❌ Some tests FAILED!")
        
        print(f"\nFinal stats:")
        print(f"  FAISS embeddings: {em.get_total_embeddings()}")
        print(f"  Database: {db.get_stats()}")
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        try:
            db.client.drop_database("hibikido_simple_test")
            db.close()
        except:
            pass
        
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("🧹 Cleaned up test data")

def test_search_handler_simulation():
    """Simulate the search handler without OSC."""
    print(f"\n🎭 Simulating Search Handler")
    print("=" * 40)
    
    # Mock OSC for simulation
    class MockOSC:
        def send_error(self, msg):
            print(f"OSC ERROR: {msg}")
        
        def send_confirm(self, msg):
            print(f"OSC CONFIRM: {msg}")
        
        class MockClient:
            def send_message(self, address, data):
                print(f"OSC SEND: {address} -> {data}")
        
        def __init__(self):
            self.client = self.MockClient()
    
    # Setup test data (reuse previous setup)
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = HibikidoDatabase(db_name="hibikido_handler_test")
        if not db.connect():
            print("❌ Database connection failed")
            return False
        
        em = EmbeddingManager(index_file=f"{temp_dir}/handler_test.index")
        if not em.initialize():
            print("❌ Embedding manager failed")
            return False
        
        # Add minimal test data
        db.add_recording("test_rec", "test.wav", "Test recording")
        db.add_segmentation("test_seg_method", "manual", {}, "Test segmentation")
        
        db.add_segment(
            segment_id="test_segment",
            source_id="test_rec",
            segmentation_id="test_seg_method",
            start=0.0, end=1.0,
            description="Test audio segment",
            embedding_text="test audio segment bright sound"
        )
        
        # Rebuild index
        stats = em.rebuild_from_database(db)
        print(f"Setup complete: {stats}")
        
        # Simulate search handler
        def simulate_search(query):
            print(f"\n🔍 Simulating /search '{query}'")
            
            # This mimics the actual search handler
            results = em.search(query, top_k=10, db_manager=db)
            
            if not results:
                print("   OSC CONFIRM: no matches found")
                return
            
            # Send each result
            for i, result in enumerate(results):
                collection = result["collection"]
                document = result["document"]
                score = result["score"]
                
                print(f"   OSC SEND: /result -> [{i}, {collection}, {score:.3f}, {str(document)}]")
            
            print(f"   OSC SEND: /search_complete -> {len(results)}")
        
        # Test various queries
        test_queries = [
            "test audio bright",
            "nonexistent query",
            "",  # Empty query (should error)
            "sound segment"
        ]
        
        for query in test_queries:
            if not query:
                print(f"\n🔍 Simulating /search '{query}'")
                print("   OSC ERROR: search requires query text")
            else:
                simulate_search(query)
        
        print(f"\n✅ Search handler simulation complete")
        return True
        
    except Exception as e:
        print(f"❌ Simulation failed: {e}")
        return False
    
    finally:
        try:
            db.client.drop_database("hibikido_handler_test")
            db.close()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    print("🚀 Running Simple Search System Tests")
    print("=" * 50)
    
    # Test 1: Basic functionality
    success1 = test_simple_search_flow()
    
    # Test 2: Search handler simulation
    success2 = test_search_handler_simulation()
    
    # Summary
    print(f"\n{'='*50}")
    print("📊 TEST SUMMARY")
    print(f"{'='*50}")
    
    tests = [
        ("Simple Search Flow", success1),
        ("Search Handler Simulation", success2)
    ]
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    for test_name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total:.1%})")
    
    if passed == total:
        print("\n🎉 All tests passed! Simple search system is working.")
        print("\nNext steps:")
        print("1. Integrate with your main server")
        print("2. Test with real Max/MSP patches")
    else:
        print(f"\n⚠️  {total - passed} tests failed")
    
    exit(0 if passed == total else 1)