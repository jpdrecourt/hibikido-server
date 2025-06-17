# Incantation Server

A semantic search engine for musical sounds and effects that maps natural language descriptions (incantations) to audio content using neural embeddings. Connect your Max/MSP patches to a smart audio database that understands poetic descriptions.

## For Musicians & Sound Designers

### What It Does

Instead of browsing folders or remembering filenames, describe what you want in natural language:

- _"ethereal forest ambience"_ → finds atmospheric nature recordings
- _"dark ritualistic drone"_ → locates deep, mysterious sustained tones
- _"metallic percussion burst"_ → discovers sharp, metallic hits
- _"warm analog pad"_ → retrieves vintage synthesizer textures

The server understands semantic relationships, so similar descriptions find related sounds even if the exact words don't match your database.

### Quick Start for Users

1. **Install & Run** (see setup below)
2. **Import Your Sounds**: Point the server to a CSV file describing your audio library
3. **Connect via OSC**: Use any OSC client to send queries and receive sound matches
4. **Cast Incantations**: Type natural descriptions and get relevant audio back

### OSC Commands Reference

Send these messages from any OSC client:

#### Core Commands

```
/search "your incantation here"
    → Returns: /matches [id, type, title, file, score, id, type, title, file, score, ...]

/import_csv "/path/to/your/sounds.csv"
    → Bulk import your sound library

/stats
    → Returns: /stats_result [total, active, deleted, with_embeddings]
```

#### Database Management

```
/add "description" "{\"file\":\"path.wav\", \"type\":\"sample\"}"
    → Add single entry with optional metadata JSON

/get_by_id 42
    → Get specific entry by ID

/soft_delete 42
    → Mark entry as deleted (preserves for undo)
```

#### System Control

```
/stop
    → Graceful server shutdown
```

### CSV Import Format

Your sound library CSV can have any columns, but these are recommended:

| Required | Column      | Description          | Example                               |
| -------- | ----------- | -------------------- | ------------------------------------- |
| ✓        | ID          | Unique number        | 1, 2, 3...                            |
| ✓        | File        | Audio file path      | "drums/kick_01.wav"                   |
|          | Title       | Human-readable name  | "Deep Kick Drum"                      |
|          | Description | Detailed description | "Punchy 808-style kick with sub bass" |
|          | Type        | Category             | "drum", "effect", "loop"              |
|          | Duration    | Length in seconds    | 2.5                                   |
|          | Location    | Recording location   | "Studio A"                            |
|          | Gear        | Equipment used       | "Neumann U87, SSL Console"            |

**The server is completely schemaless** - add any columns you want. They'll be stored and searchable.

### Response Format

**Search Results** (`/matches`):
Flat array of: `[id1, type1, title1, file1, score1, id2, type2, title2, file2, score2, ...]`

- **id**: Database ID number
- **type**: Content category
- **title**: Display name
- **file**: Audio file path
- **score**: Similarity score (0.0-1.0, higher = better match)

**Status Messages**:

- `/confirm "message"` - Success notifications
- `/error "message"` - Error descriptions
- `/stats_result [total, active, deleted, with_embeddings]` - Database stats

### Tips for Better Results

1. **Rich Descriptions**: Include mood, texture, instrument, genre

   - Good: _"bright acoustic guitar strumming folk style"_
   - Basic: _"guitar"_

2. **Consistent Terminology**: Use similar words across your database

   - _"percussion"_ vs _"drums"_ vs _"beats"_

3. **Semantic Variety**: The AI understands synonyms and related concepts

   - _"ethereal"_ matches _"atmospheric"_, _"spacious"_, _"ambient"_

4. **Multi-word Queries**: Longer descriptions often yield better results
   - _"dark industrial metal scraping"_ is more specific than _"metal"_

---

## For Developers

### Architecture Overview

```
Max/MSP ←→ OSC ←→ main_server.py ←→ MongoDB + FAISS Vector DB
                        ↓
            [database_manager, embedding_manager, text_processor, csv_importer]
```

### Core Components

```
main_server.py          # Central orchestrator, OSC command routing
├── database_manager.py  # MongoDB operations, schemaless storage
├── embedding_manager.py # FAISS vector search, sentence transformers
├── text_processor.py   # Text cleaning, semantic enhancement
├── csv_importer.py     # Bulk import with intelligent updates
└── osc_handler.py      # OSC protocol implementation
```

### Installation & Setup

#### Dependencies

```bash
pip install sentence-transformers python-osc faiss-cpu torch pymongo pandas

# Optional for enhanced text processing:
pip install spacy
python -m spacy download en_core_web_sm
```

#### MongoDB Setup

Install MongoDB Community Server and ensure it's running:

```bash
# Default connection: mongodb://localhost:27017
mongod
```

#### Launch Server

```bash
python main_server.py [--config config.json] [--log-level DEBUG]
```

### Data Pipeline

#### Import Flow

```
CSV File → csv_importer.py → text_processor.py → embedding_manager.py → database_manager.py
     ↓              ↓                 ↓                    ↓                    ↓
Field Extract → Text Clean → Sentence Embed → FAISS Index → MongoDB Store
```

#### Add Entry Flow

```
OSC /add → main_server.py → text_processor.py → embedding_manager.py → database_manager.py
    ↓            ↓                   ↓                    ↓                    ↓
Parse Args → Create Entry → Generate Embed → FAISS Index → MongoDB Store
```

#### Search Flow

```
User Query → text_processor.py → embedding_manager.py → database_manager.py → OSC Response
      ↓             ↓                      ↓                     ↓                ↓
   Enhance → Create Vector → FAISS Search → Lookup Metadata → Format Results
```

### Database Schema

**Core System Fields** (required):

```javascript
{
  "_id": ObjectId("..."),
  "ID": 42,                          // Unique identifier
  "embedding_text": "processed text for semantic search",
  "faiss_id": 156,                   // Vector database index
  "created_at": ISODate("..."),
  "deleted": false
}
```

**User Data Fields** (completely flexible):

```javascript
{
  // Any CSV columns stored as-is
  "title": "Steel Ball On Marble",
  "description": "Bouncing a small steel ball on marble surface",
  "file": "percussion/steel_ball_01.wav",
  "type": "foley",
  "duration": 12.5,
  "location": "Studio A",
  "gear": "Zoom H6, AKG C414",
  "mood": "playful",
  "bpm": 120,
  // ... literally any other fields
}
```

### Key Design Principles

1. **Schemaless Storage**: Only 5 core fields required, everything else flexible
2. **Soft Deletes**: Entries marked deleted, never physically removed
3. **Update-Safe Imports**: Re-importing updates metadata but preserves embeddings
4. **Semantic Text Processing**: Intelligent text cleaning and enhancement
5. **Vector Similarity**: FAISS for fast, accurate semantic search

### Component Deep Dive

#### text_processor.py

**Purpose**: Converts raw CSV data into optimized embedding text.

**Key Methods**:

- `create_embedding_sentence()`: Combines title + description + filename intelligently
- `enhance_query()`: Improves user queries with spaCy processing
- `_clean_text()`: Normalizes text (removes special chars, lowercases, etc.)

**Text Processing Pipeline**:

```
Raw CSV → Extract Fields → Clean Text → spaCy Enhancement → Embedding Ready
```

#### embedding_manager.py

**Purpose**: Neural embeddings and vector similarity search.

**Configuration**:

- Model: `all-MiniLM-L6-v2` (384-dim embeddings)
- Index: FAISS `IndexFlatIP` with cosine similarity
- Storage: Persistent index file with auto-save

**Key Methods**:

- `encode_text()`: Text → normalized embedding vector
- `add_embedding()`: Store vector with duplicate detection
- `search()`: Query → ranked similarity results

#### database_manager.py

**Purpose**: MongoDB interface with flexible schema handling.

**Indexing Strategy**:

```python
# Performance indexes
collection.create_index("faiss_id", unique=True)
collection.create_index("ID", unique=True)
collection.create_index("deleted")
collection.create_index([("title", "text"), ("description", "text")])
```

**Key Methods**:

- `add_entry()`: Validates core fields, stores complete document
- `get_by_faiss_id()`: Retrieval for search results
- `soft_delete()`: Safe deletion with undo capability

#### csv_importer.py

**Purpose**: Intelligent bulk import with update handling.

**Import Logic**:

1. Validate CSV structure and required fields
2. For each row: extract ALL fields (completely schemaless)
3. Check if entry exists by ID
4. **Update existing**: Preserve embeddings, update metadata
5. **Add new**: Full processing with embedding generation

#### osc_handler.py

**Purpose**: OSC protocol implementation.

**Message Flow**:

```
OSC Input → parse_args() → Dispatcher → Handler Method → Format Response → OSC Output
```

**Response Types**:

- `/matches`: Search results as flat array
- `/confirm`: Status messages
- `/error`: Error descriptions
- `/stats_result`: Database analytics

### Configuration

Create `config.json` to override defaults:

```json
{
  "mongodb": {
    "uri": "mongodb://localhost:27017",
    "database": "incantations",
    "collection": "entries"
  },
  "embedding": {
    "model_name": "all-MiniLM-L6-v2",
    "index_file": "incantations.index"
  },
  "osc": {
    "listen_ip": "127.0.0.1",
    "listen_port": 9000,
    "send_ip": "127.0.0.1",
    "send_port": 9001
  },
  "search": {
    "top_k": 10,
    "min_score": 0.3
  }
}
```

### Performance Characteristics

- **Search Speed**: ~1-15ms depending on database size
- **Embedding Generation**: ~10-50ms per entry
- **Bulk Import**: ~20-100 entries/second
- **Memory Usage**: ~100MB for model + index
- **Scalability**: Tested up to 100K entries

### Development Workflow

#### Adding New OSC Commands

1. Add handler method in `main_server.py`
2. Register in `_register_osc_handlers()`
3. Add OSC address to `osc_handler.py`

#### Extending Text Processing

1. Modify `TextProcessor` methods in `text_processor.py`
2. Update `create_embedding_sentence()` logic
3. Test with sample CSV data

#### Database Schema Extensions

1. Update validation in `database_manager.py`
2. Add indexes for performance
3. Create migration scripts if needed

### Testing Strategy

**Unit Tests**: Individual component methods, edge cases
**Integration Tests**: Full import/search workflows  
**Performance Tests**: Large datasets, concurrent requests

### Debugging Tips

1. **Enable DEBUG logging**: `--log-level DEBUG`
2. **Check embedding quality**: Examine `embedding_text` field in MongoDB
3. **Validate CSV structure**: Use `/import_csv` validation
4. **Monitor FAISS index**: Check `incantations.index` file size
5. **OSC debugging**: Use tools like `oscdump` to monitor traffic

---

## Troubleshooting

### Common Issues

**No search results**:

- Check if embeddings were created (`/stats` should show entries with embeddings)
- Verify `embedding_text` field in database entries
- Try broader/simpler queries

**Import failures**:

- Ensure CSV has `ID` column with unique integers
- Check file encoding (UTF-8 recommended)
- Verify MongoDB connection

**OSC connectivity**:

- Confirm ports 9000/9001 are available
- Check firewall settings
- Test with simple OSC tools first

**Performance issues**:

- Monitor MongoDB indexes with `.explain()`
- Check FAISS index size vs. available memory
- Consider model size vs. accuracy tradeoffs
