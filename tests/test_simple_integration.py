"""
Simple Integration Test (Updated for Path-based Schema)
=======================================================

Test the simplified search system: OSC -> FAISS -> MongoDB -> OSC
"""

import tempfile
import shutil
from hibikido.database_manager import HibikidoDatabase
from hibikido.embedding_manager import EmbeddingManager

def test_simple_search_flow():
    """Test the complete simple search flow with path-based schema."""
    print("🧪 Testing Simple Search Flow (Path-based)")
    print("=" * 50)
    
    # Setup
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Initialize database
        db = HibikidoDatabase(db_name="hibikido_simple_test")
        if not db.connect():
            print("❌ Failed to connect to MongoDB")
            assert False
        
        # Initialize embedding manager
        index_file = f"{temp_dir}/simple_test.index"
        
        em = EmbeddingManager(index_file=index_file)
        
        if not em.initialize():
            print("❌ Failed to initialize embedding manager")
            assert False
        
        print("✅ Database and embedding manager initialized")
        
        # Add test data using path-based schema
        print("\n📝 Adding test data...")
        
        # Add recordings with paths
        recording_paths = [
            "sounds/nature/forest_morning.wav",
            "sounds/urban/city_ambience.wav"
        ]
        
        for path in recording_paths:
            db.add_recording(path, f"Recording: {path.split('/')[-1]}")
        
        # Add segmentation
        db.add_segmentation("manual_v1", "manual", {}, "Manual segmentation")
        
        # Add segments (with normalized 0-1 values, referencing by path)
        segments = [
            {
                "source_path": "sounds/nature/forest_morning.wav",
                "start": 0.0, "end": 0.4,
                "description": "Robin territorial call",
                "embedding_text": "bright robin territorial call morning"
            },
            {
                "source_path": "sounds/nature/forest_morning.wav",
                "start": 0.5, "end": 0.9,
                "description": "Blackbird melodic song", 
                "embedding_text": "melodic blackbird complex musical phrase"
            },
            {
                "source_path": "sounds/urban/city_ambience.wav",
                "start": 0.0, "end": 1.0,
                "description": "Urban atmosphere",
                "embedding_text": "peaceful urban atmosphere ambient traffic"
            }
        ]
        
        for seg in segments:
            success = db.add_segment(
                source_path=seg["source_path"],
                segmentation_id="manual_v1", 
                start=seg["start"],
                end=seg["end"],
                description=seg["description"],
                embedding_text=seg["embedding_text"]
            )
            assert success, f"Failed to add segment: {seg['description']}"
        
        # Add effects with paths
        effect_paths = [
            "effects/reverb/cathedral.maxpat",
            "effects/granular/processor.maxpat"
        ]
        
        for path in effect_paths:
            name = path.split('/')[-1].split('.')[0]
            db.add_effect(path, name, f"Effect: {name}")
        
        # Add presets (to separate collection, referencing by effect_path)
        presets = [
            {
                "effect_path": "effects/reverb/cathedral.maxpat",
                "parameters": [0.8, 0.4, 0.9],
                "description": "Cathedral reverb",
                "embedding_text": "cathedral spacious reverb long decay sacred"
            },
            {
                "effect_path": "effects/granular/processor.maxpat", 
                "parameters": [0.05, 0.2],
                "description": "Ethereal granular texture",
                "embedding_text": "ethereal granular texture atmospheric dreamy"
            }
        ]
        
        for preset in presets:
            success = db.add_preset(
                effect_path=preset["effect_path"],
                parameters=preset["parameters"],
                description=preset["description"],
                embedding_text=preset["embedding_text"]
            )
            assert success, f"Failed to add preset: {preset['description']}"
        
        print(f"✅ Added {len(segments)} segments and {len(presets)} presets")
        
        # Rebuild FAISS index from database
        print("\n🔄 Building FAISS index...")
        stats = em.rebuild_from_database(db)
        print(f"📊 Build stats: {stats}")
        
        expected_total = len(segments) + len(presets)  # 3 segments + 2 presets = 5
        if em.get_total_embeddings() != expected_total:
            print(f"❌ Expected {expected_total} embeddings, got {em.get_total_embeddings()}")
            assert False
        
        print("✅ FAISS index built successfully")
        
        # Test searches
        print("\n🔍 Testing searches...")
        
        test_queries = [
            {
                "query": "bright bird call",
                "expected_collections": ["segments"],
                "expected_contains": ["robin", "territorial"]
            },
            {
                "query": "melodic musical phrase", 
                "expected_collections": ["segments"],
                "expected_contains": ["blackbird", "melodic"]
            },
            {
                "query": "cathedral spacious reverb",
                "expected_collections": ["presets"],
                "expected_contains": ["cathedral", "reverb"]
            },
            {
                "query": "ethereal granular texture",
                "expected_collections": ["presets"],
                "expected_contains": ["ethereal", "granular"]
            },
            {
                "query": "urban atmosphere ambient",
                "expected_collections": ["segments"],
                "expected_contains": ["urban", "atmosphere"]
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
                    path = doc.get("source_path", "unknown")
                    start = doc.get("start", 0.0)
                    end = doc.get("end", 1.0)
                    desc = doc.get("description", "no description")
                    print(f"       {j+1}. [segment] {path} [{start:.1f}-{end:.1f}] {desc} (score: {score:.3f})")
                elif collection == "presets":
                    path = doc.get("effect_path", "unknown")
                    params = doc.get("parameters", [])
                    desc = doc.get("description", "no description")
                    print(f"       {j+1}. [preset] {path} {params} {desc} (score: {score:.3f})")
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
        
        # Test path-based MongoDB integration
        print(f"\n🗄️  Testing path-based MongoDB integration...")
        
        # Test segment retrieval by path
        forest_segments = db.get_segments_by_recording_path("sounds/nature/forest_morning.wav")
        if len(forest_segments) == 2:
            print("✅ Segments found by recording path")
        else:
            print(f"❌ Expected 2 segments, found {len(forest_segments)}")
            all_passed = False
        
        # Test preset retrieval by path
        reverb_presets = db.get_presets_by_effect_path("effects/reverb/cathedral.maxpat")
        if len(reverb_presets) == 1:
            print("✅ Presets found by effect path")
        else:
            print(f"❌ Expected 1 preset, found {len(reverb_presets)}")
            all_passed = False
        
        # Test FAISS index integration
        segments_with_faiss = db.get_segments_by_recording_path("sounds/nature/forest_morning.wav")
        faiss_count = sum(1 for s in segments_with_faiss if "FAISS_index" in s)
        if faiss_count == 2:
            print("✅ Segments have FAISS indices")
        else:
            print(f"❌ Expected 2 segments with FAISS indices, got {faiss_count}")
            all_passed = False
        
        presets_with_faiss = db.get_presets_by_effect_path("effects/reverb/cathedral.maxpat")
        faiss_preset_count = sum(1 for p in presets_with_faiss if "FAISS_index" in p)
        if faiss_preset_count == 1:
            print("✅ Presets have FAISS indices")
        else:
            print(f"❌ Expected 1 preset with FAISS index, got {faiss_preset_count}")
            all_passed = False
        
        # Test persistence
        print(f"\n💾 Testing persistence...")
        
        # Create new embedding manager to test loading
        em2 = EmbeddingManager(index_file=index_file)
        
        if em2.initialize() and em2.get_total_embeddings() == expected_total:
            print("✅ Index loaded successfully")
        else:
            print(f"❌ Failed to load index - expected {expected_total}, got {em2.get_total_embeddings()}")
            all_passed = False
        
        # Test search on loaded index
        loaded_results = em2.search("bright bird", top_k=3, db_manager=db)
        if loaded_results and "robin" in str(loaded_results[0]["document"]).lower():
            print("✅ Search works on loaded index")
        else:
            print("❌ Search failed on loaded index")
            all_passed = False
        
        # Test normalized values in results
        print(f"\n📏 Testing normalized segment values...")
        
        robin_results = em.search("robin territorial", top_k=1, db_manager=db)
        if robin_results and robin_results[0]["collection"] == "segments":
            segment = robin_results[0]["document"]
            start = segment.get("start", -1)
            end = segment.get("end", -1)
            
            if 0.0 <= start <= 1.0 and 0.0 <= end <= 1.0 and start < end:
                print(f"✅ Normalized values: start={start}, end={end}")
            else:
                print(f"❌ Invalid normalized values: start={start}, end={end}")
                all_passed = False
        else:
            print("❌ No robin segment found for normalization test")
            all_passed = False
        
        # Summary
        print(f"\n{'='*50}")
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
        assert False
    
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
    """Simulate the search handler without OSC using path-based schema."""
    print(f"\n🎭 Simulating Search Handler (Path-based)")
    print("=" * 50)
    
    # Mock OSC for simulation
    class MockOSC:
        def send_error(self, msg):
            print(f"OSC ERROR: {msg}")
        
        def send_confirm(self, msg):
            print(f"OSC CONFIRM: {msg}")
        
        class MockClient:
            def send_message(self, address, data):
                if address == "/result":
                    index, collection, score, path, description, start, end, parameters = data
                    if collection == "segments":
                        print(f"OSC RESULT: {index} [segment] {path} [{start:.1f}-{end:.1f}] '{description}' (score: {score:.3f})")
                    else:
                        print(f"OSC RESULT: {index} [preset] {path} {parameters} '{description}' (score: {score:.3f})")
                else:
                    print(f"OSC SEND: {address} -> {data}")
        
        def __init__(self):
            self.client = self.MockClient()
    
    # Mock text processor for description generation
    class MockTextProcessor:
        def _create_display_description(self, embedding_text):
            if not embedding_text:
                return "untitled"
            words = embedding_text.split()[:4]
            return " ".join(words).title()
    
    # Setup test data
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = HibikidoDatabase(db_name="hibikido_handler_test")
        if not db.connect():
            print("❌ Database connection failed")
            assert False
        
        em = EmbeddingManager(index_file=f"{temp_dir}/handler_test.index")
        if not em.initialize():
            print("❌ Embedding manager failed")
            assert False
        
        text_processor = MockTextProcessor()
        
        # Add minimal test data using path-based schema
        recording_path = "test/audio/sample.wav"
        effect_path = "test/effects/reverb.maxpat"
        
        db.add_recording(recording_path, "Test recording for handler simulation")
        db.add_segmentation("test_seg_method", "manual", {}, "Test segmentation")
        
        # Add segment
        success = db.add_segment(
            source_path=recording_path,
            segmentation_id="test_seg_method",
            start=0.2,  # Normalized value
            end=0.8,    # Normalized value
            description="Test audio segment",
            embedding_text="test audio segment bright clear sound"
        )
        assert success, "Failed to add test segment"
        
        # Add effect and preset
        db.add_effect(effect_path, "Test Reverb", "Test reverb effect")
        success = db.add_preset(
            effect_path=effect_path,
            parameters=[0.7, 0.3],
            description="Test reverb preset",
            embedding_text="test reverb preset warm atmospheric"
        )
        assert success, "Failed to add test preset"
        
        # Rebuild index
        stats = em.rebuild_from_database(db)
        print(f"Setup complete: {stats}")
        
        # Simulate simplified search handler with uniform output
        def simulate_search(query):
            print(f"\n🔍 Simulating /search '{query}'")
            
            if not query or not query.strip():
                print("   OSC ERROR: search requires query text")
                return
            
            # This mimics the actual simplified search handler
            results = em.search(query, top_k=10, db_manager=db)
            
            if not results:
                print("   OSC CONFIRM: no matches found")
                return
            
            # Send each result in simplified format
            for i, result in enumerate(results):
                collection = result["collection"]
                document = result["document"]
                score = result["score"]
                
                # Extract simplified fields
                if collection == "segments":
                    path = document.get("source_path", "")
                    start = document.get("start", 0.0)
                    end = document.get("end", 1.0)
                    parameters = []  # Empty for segments
                else:  # presets
                    path = document.get("effect_path", "")
                    start = 0.0  # Default for presets
                    end = 0.0    # Default for presets  
                    parameters = document.get("parameters", [])
                
                # Create description
                embedding_text = document.get("embedding_text", "")
                description = text_processor._create_display_description(embedding_text)
                
                # Send simplified result
                mock_osc = MockOSC()
                mock_osc.client.send_message("/result", [
                    i,                                    # index
                    collection,                           # collection
                    float(score),                         # score
                    str(path),                           # path
                    str(description),                    # description
                    float(start),                        # start (normalized for segments, 0.0 for presets)
                    float(end),                          # end (normalized for segments, 0.0 for presets)
                    str(parameters) if parameters else "[]"  # parameters (JSON string)
                ])
            
            print(f"   OSC SEND: /search_complete -> {len(results)}")
        
        # Test various queries
        test_queries = [
            "test audio bright",
            "warm atmospheric reverb",
            "nonexistent query",
            "",  # Empty query (should error)
            "clear sound segment"
        ]
        
        for query in test_queries:
            simulate_search(query)
        
        print(f"\n✅ Search handler simulation complete")
        assert True
        
    except Exception as e:
        print(f"❌ Simulation failed: {e}")
        assert False
    
    finally:
        try:
            db.client.drop_database("hibikido_handler_test")
            db.close()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_path_based_workflow():
    """Test the complete path-based workflow."""
    print(f"\n🛤️  Testing Complete Path-based Workflow")
    print("=" * 50)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = HibikidoDatabase(db_name="hibikido_workflow_test")
        if not db.connect():
            print("❌ Database connection failed")
            assert False
        
        em = EmbeddingManager(index_file=f"{temp_dir}/workflow_test.index")
        if not em.initialize():
            print("❌ Embedding manager failed")
            assert False
        
        print("✅ System initialized")
        
        # Step 1: Artist adds recordings using paths
        print("\n1️⃣  Adding recordings by path...")
        
        recordings = [
            ("sounds/nature/forest_dawn.wav", "Morning forest with bird chorus"),
            ("sounds/urban/street_ambience.wav", "Urban street with traffic and voices"),
            ("sounds/experimental/drone_texture.wav", "Deep synthetic drone texture")
        ]
        
        for path, desc in recordings:
            success = db.add_recording(path, desc)
            print(f"   {'✅' if success else '❌'} {path}")
        
        # Step 2: Artist segments recordings using normalized values
        print("\n2️⃣  Adding segments with normalized timing...")
        
        segments = [
            {
                "source_path": "sounds/nature/forest_dawn.wav",
                "start": 0.0, "end": 0.3,
                "description": "Robin morning call",
                "embedding_text": "robin morning call bright territorial"
            },
            {
                "source_path": "sounds/nature/forest_dawn.wav", 
                "start": 0.4, "end": 0.7,
                "description": "Blackbird complex song",
                "embedding_text": "blackbird complex song melodic musical"
            },
            {
                "source_path": "sounds/urban/street_ambience.wav",
                "start": 0.0, "end": 1.0,
                "description": "Urban atmosphere",
                "embedding_text": "urban atmosphere traffic voices city"
            }
        ]
        
        for seg in segments:
            success = db.add_segment(
                source_path=seg["source_path"],
                segmentation_id="artist_manual",
                start=seg["start"],
                end=seg["end"], 
                description=seg["description"],
                embedding_text=seg["embedding_text"]
            )
            print(f"   {'✅' if success else '❌'} {seg['description']} [{seg['start']:.1f}-{seg['end']:.1f}]")
        
        # Step 3: Artist adds effects using paths
        print("\n3️⃣  Adding effects by path...")
        
        effects = [
            ("effects/reverb/cathedral.maxpat", "Cathedral Reverb", "Spacious cathedral reverb processor"),
            ("effects/granular/texture.maxpat", "Granular Texture", "Real-time granular synthesis")
        ]
        
        for path, name, desc in effects:
            success = db.add_effect(path, name, desc)
            print(f"   {'✅' if success else '❌'} {path}")
        
        # Step 4: Artist creates presets for effects
        print("\n4️⃣  Adding presets with parameters...")
        
        presets = [
            {
                "effect_path": "effects/reverb/cathedral.maxpat",
                "parameters": [0.8, 0.4, 0.9],
                "description": "Warm cathedral ambience",
                "embedding_text": "warm cathedral ambience spacious sacred reverb"
            },
            {
                "effect_path": "effects/granular/texture.maxpat",
                "parameters": [0.05, 0.2, 2.0],
                "description": "Ethereal time stretch",
                "embedding_text": "ethereal time stretch granular atmospheric"
            }
        ]
        
        for preset in presets:
            success = db.add_preset(
                effect_path=preset["effect_path"],
                parameters=preset["parameters"],
                description=preset["description"],
                embedding_text=preset["embedding_text"]
            )
            print(f"   {'✅' if success else '❌'} {preset['description']}")
        
        # Step 5: Build searchable index
        print("\n5️⃣  Building semantic search index...")
        
        stats = em.rebuild_from_database(db)
        total_items = stats["segments_added"] + stats["presets_added"]
        print(f"   📊 Indexed {total_items} items ({stats['segments_added']} segments, {stats['presets_added']} presets)")
        
        # Step 6: Test semantic search
        print("\n6️⃣  Testing semantic search...")
        
        test_searches = [
            "bright morning bird call",
            "spacious cathedral reverb",
            "urban city atmosphere",
            "ethereal granular texture"
        ]
        
        for query in test_searches:
            results = em.search(query, top_k=2, db_manager=db)
            print(f"\n   🔍 '{query}':")
            
            for i, result in enumerate(results):
                collection = result["collection"]
                score = result["score"]
                doc = result["document"]
                
                if collection == "segments":
                    path = doc["source_path"]
                    start = doc["start"]
                    end = doc["end"]
                    desc = doc["description"]
                    print(f"      {i+1}. [segment] {path} [{start:.1f}-{end:.1f}] {desc} ({score:.3f})")
                else:
                    path = doc["effect_path"]
                    params = doc["parameters"]
                    desc = doc["description"]
                    print(f"      {i+1}. [preset] {path} {params} {desc} ({score:.3f})")
        
        # Step 7: Verify artist workflow requirements
        print("\n7️⃣  Verifying artist workflow...")
        
        # Check paths as identifiers work
        forest_recording = db.get_recording_by_path("sounds/nature/forest_dawn.wav")
        cathedral_effect = db.get_effect_by_path("effects/reverb/cathedral.maxpat")
        
        if forest_recording and cathedral_effect:
            print("   ✅ Path-based lookups working")
        else:
            print("   ❌ Path-based lookups failed")
            assert False
        
        # Check normalized segment values
        forest_segments = db.get_segments_by_recording_path("sounds/nature/forest_dawn.wav")
        all_normalized = all(
            0.0 <= seg["start"] <= 1.0 and 0.0 <= seg["end"] <= 1.0 
            for seg in forest_segments
        )
        
        if all_normalized:
            print("   ✅ All segments use normalized 0-1 values")
        else:
            print("   ❌ Some segments have non-normalized values")
            assert False
        
        # Check separate presets collection
        cathedral_presets = db.get_presets_by_effect_path("effects/reverb/cathedral.maxpat")
        
        if len(cathedral_presets) == 1 and cathedral_presets[0]["effect_path"] == "effects/reverb/cathedral.maxpat":
            print("   ✅ Separate presets collection working")
        else:
            print("   ❌ Separate presets collection failed")
            assert False
        
        print("\n🎉 Complete path-based workflow test PASSED!")
        assert True
        
    except Exception as e:
        print(f"❌ Workflow test failed: {e}")
        import traceback
        traceback.print_exc()
        assert False
    
    finally:
        try:
            db.client.drop_database("hibikido_workflow_test")
            db.close()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    print("🚀 Running Path-based Search System Tests")
    print("=" * 60)
    
    # Test 1: Basic functionality with path-based schema
    success1 = test_simple_search_flow()
    
    # Test 2: Search handler simulation with simplified output
    success2 = test_search_handler_simulation()
    
    # Test 3: Complete path-based workflow
    success3 = test_path_based_workflow()
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 TEST SUMMARY")
    print(f"{'='*60}")
    
    tests = [
        ("Path-based Search Flow", success1),
        ("Simplified Search Handler", success2),
        ("Complete Path-based Workflow", success3)
    ]
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    for test_name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total:.1%})")
    
    if passed == total:
        print("\n🎉 All tests passed! Path-based search system is working.")
        print("\nKey features verified:")
        print("- Path-based recording/effect identification")
        print("- Normalized 0-1 segment timing")
        print("- Separate presets collection")
        print("- Simplified search results") 
        print("- Semantic search with FAISS")
        print("\nReady for OSC integration!")
    else:
        print(f"\n⚠️  {total - passed} tests failed")
    
    exit(0 if passed == total else 1)