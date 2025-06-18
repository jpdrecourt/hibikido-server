// MongoDB JSON Schema for Hibikidō Database Design (converted to JS-compatible syntax with comments)
// This schema defines collections used to support an emergent, poetic, and reactive sound system.
// Comments reflect intended use and clarify data structures for future maintenance and semantic search logic.

// eslint-disable-next-line no-unused-vars
const schemas = {
  // Recordings: Each document refers to a single source file (typically a field recording or raw material).
  // This is the immutable root unit. Each will be referenced by multiple segments.
  recordings: {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'path', 'description'],
        properties: {
          _id: { bsonType: 'string' }, // Unique identifier for the recording
          path: { bsonType: 'string' }, // Relative path to audio file
          description: { bsonType: 'string' }, // Short poetic or factual description of the source material
        },
      },
    },
  },

  // Segments: Describes meaningful sonic slices of a recording, typically normalized to [0–1] for precision and duration independence.
  // Each segment is individually embedded and semantically active in search/invocation.
  segments: {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: [
          '_id',
          'source_id',
          'start',
          'end',
          'description',
          'embedding_text',
          'FAISS_index',
          'segmentation_id',
        ],
        properties: {
          _id: { bsonType: 'string' },
          source_id: { bsonType: 'string' }, // Reference to a recording
          segmentation_id: { bsonType: 'string' }, // Reference to a segmentation method or run
          start: { bsonType: 'double' }, // Start time relative to duration (0.0 inclusive)
          end: { bsonType: 'double' }, // End time relative to duration (1.0 exclusive)
          description: { bsonType: 'string' }, // Description of the segment content or mood
          embedding_text: { bsonType: 'string' }, // Textual representation based the descriptions for the recording, the segment and the segmentation
          FAISS_index: { bsonType: 'int' }, // Position in FAISS vector store
        },
      },
    },
  },

  // Effects: Includes dynamic processes (FX chains, plugins, transformations) with optional presets.
  // Embedding is applied to presets, not effects themselves, allowing high-resolution modulation.
  effects: {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'name', 'path'],
        properties: {
          _id: { bsonType: 'string' },
          name: { bsonType: 'string' }, // Human-readable FX label
          path: { bsonType: 'string' }, // Path to executable FX or preset definition
          description: { bsonType: 'string' },
          presets: {
            bsonType: 'array',
            items: {
              bsonType: 'object',
              required: [
                'parameters',
                'description',
                'embedding_text',
                'FAISS_index',
              ],
              properties: {
                parameters: {
                  bsonType: 'array',
                  items: {
                    bsonType: 'object',
                    required: ['name', 'value'],
                    properties: {
                      name: { bsonType: 'string' }, // Parameter name (matches VST/Max mapping)
                      value: { bsonType: 'double' }, // Typically index for enum or direct control value
                    },
                  },
                },
                description: { bsonType: 'string' }, // Human-readable purpose or effect profile
                embedding_text: { bsonType: 'string' }, // Textual representation used for semantic matching
                FAISS_index: { bsonType: 'int' }, // FAISS vector reference
              },
            },
          },
        },
      },
    },
  },

  // Performances: Minimal log of poetic or command-based invocations and their resolved sound/action outputs.
  // This structure will grow over time but remains lightweight to begin with.
  performances: {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'date'],
        properties: {
          _id: { bsonType: 'string' }, // Performance session ID
          date: { bsonType: 'date' }, // ISODate of performance or recording
          invocations: {
            bsonType: 'array',
            items: {
              bsonType: 'object',
              properties: {
                text: { bsonType: 'string' }, // Raw invocation phrase or prompt
                segment_id: { bsonType: 'string' }, // Matched segment (if any)
                effect: { bsonType: 'string' }, // Matched effect (if any)
                time: { bsonType: 'double' }, // Time into the session (seconds)
              },
            },
          },
        },
      },
    },
  },

  // Segmentations: Describes how a batch of segments was derived, including method, parameters, and semantic role.
  // Enables future understanding of the logic behind extracted sonic units.
  segmentations: {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'method'],
        properties: {
          _id: { bsonType: 'string' }, // Unique segmentation run or strategy ID
          method: { bsonType: 'string' }, // Named method, e.g., 'manual', 'transient_detector', 'AI-cluster'
          parameters: {
            bsonType: 'object', // Method-specific parameters (thresholds, models, etc.)
          },
          description: { bsonType: 'string' }, // Human description for documentation, compositional notes
        },
      },
    },
  },
}
