#!/usr/bin/env python3
"""
OSC ↔ Sentence-Transformers + Unified FAISS matching (CPU FAISS, GPU embedding)
===============================================================================

Receives OSC messages with a text query at `/text` and returns the top-N nearest
neighbors from a single FAISS index that includes entries of type 'command', 'sample',
'effect', etc. Metadata is stored in parallel in `entries.json`. Supports adding new
entries via `/add` with a type and optional metadata.

Dependencies:
    pip install sentence-transformers python-osc faiss-cpu torch

OSC Commands:
  • /text "poetic description"
  • /add "text" - adds embedding and returns FAISS ID
  • /stop - gracefully shuts down the server
  • /list_ids - returns all existing FAISS IDs in the index
  • Note: Use MongoDB soft delete instead of FAISS deletion
  • Returns via /matches: [type1, text1, score1, type2, text2, score2, ...]
  • Returns via /confirm: "added:faiss_id:normalized_text" or "duplicate:existing_id:normalized_text"
"""
from __future__ import annotations

import signal
import sys
import os
from typing import List

import numpy as np
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_server import BlockingOSCUDPServer
from pythonosc.udp_client import SimpleUDPClient

from sentence_transformers import SentenceTransformer
import faiss
import torch

# ─── Configuration ──────────────────────────────────────────────────────────
LISTEN_IP = "127.0.0.1"
LISTEN_PORT = 9000
SEND_IP = "127.0.0.1"
SEND_PORT = 9001

IN_TEXT_ADDRESS = "/text"
IN_ADD_ADDRESS = "/add"
IN_STOP_ADDRESS = "/stop"
IN_LIST_IDS_ADDRESS = "/list_ids"
OUT_ADDRESS = "/matches"
CONFIRM_ADDRESS = "/confirm"
IDS_ADDRESS = "/ids"

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 5
INDEX_FILE = "unified.index"

model = None
index = None
client = None
next_id = 0  # Simple sequential ID assignment

# ─── Initialise model, FAISS index, and OSC client ─────────────────────────
def setup():
    global model, index, client, next_id

    print(f"[INIT] Loading embedding model")
    try:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model = SentenceTransformer(MODEL_NAME, device=device, local_files_only=True)
        print(f"[INIT] Embedding model loaded on: {device.upper()}")
    except Exception as e:
        print(f"[WARN] Failed to load model on GPU, falling back to CPU: {e}")
        device = "cpu"
        model = SentenceTransformer(MODEL_NAME, device=device)
        print("[INIT] Embedding model loaded on: CPU")

    client = SimpleUDPClient(SEND_IP, SEND_PORT)

    if os.path.exists(INDEX_FILE):
        index = faiss.read_index(INDEX_FILE)
        next_id = index.ntotal  # Resume from where we left off
        faiss_device = "CPU (loaded from disk)"
        print(f"[INIT] Loaded index with {index.ntotal} entries")
    else:
        # Create simple flat index - sequential IDs work perfectly
        index = faiss.IndexFlatIP(384)
        next_id = 0
        faiss_device = "CPU (new)"
        faiss.write_index(index, INDEX_FILE)

    print(f"[INIT] FAISS index initialized on: {faiss_device}")
    print(f"[INIT] Next FAISS ID will be: {next_id}")

# ─── Helper functions ──────────────────────────────────────────────────────

# Removed get_existing_ids() function - now using in-memory set

# ─── OSC Callbacks ─────────────────────────────────────────────────────────

def handle_text(_: str, *args: List[str]) -> None:
    """Process a /text query: lower-case input, embed, search, return kebab-case matches."""
    if not args:
        print("[WARN] Received /text with no arguments.")
        return

    text = str(args[0]).lower().strip()
    print(f"[INFO] Query: {text!r}")

    if index.ntotal == 0:
        print("[WARN] Index is empty.")
        return

    try:
        query = model.encode(text, normalize_embeddings=True).reshape(1, -1)
        scores, indices = index.search(query, min(TOP_K, index.ntotal))
    except Exception as exc:
        print(f"[ERROR] Matching failed: {exc}")
        return

    flat = []
    for i in range(len(indices[0])):
        flat.extend([int(indices[0][i]), float(scores[0][i])])

    client.send_message(OUT_ADDRESS, flat)
    print(f"[INFO] Sent {len(flat)//2} matches.")

def handle_add(_: str, *args: List[str]) -> None:
    """Add a new embedding and return the assigned FAISS ID with normalized text."""
    global index, next_id
    if len(args) < 1:
        msg = "/add requires embedding text."
        print(f"[WARN] {msg}")
        client.send_message(CONFIRM_ADDRESS, msg)
        return

    try:
        original_text = str(args[0])
        normalized_text = original_text.strip().lower()
    except Exception as e:
        msg = f"Invalid text: {e}"
        print(f"[ERROR] {msg}")
        client.send_message(CONFIRM_ADDRESS, msg)
        return

    print(f"[INFO] Adding text to FAISS: original={original_text!r}, normalized={normalized_text!r}")
    
    try:
        embedding = model.encode(normalized_text, normalize_embeddings=True).reshape(1, -1)
        
        # Check for exact duplicates by searching for similarity 1.0
        if index.ntotal > 0:
            scores, indices = index.search(embedding, min(10, index.ntotal))
            # Check if any result has perfect similarity (1.0)
            for i, score in enumerate(scores[0]):
                if abs(score - 1.0) < 1e-6:  # Very close to 1.0
                    duplicate_id = int(indices[0][i])
                    msg = f"duplicate:{duplicate_id}:{normalized_text}"
                    print(f"[WARN] Duplicate detected: FAISS ID {duplicate_id} has similarity 1.0")
                    client.send_message(CONFIRM_ADDRESS, msg)
                    return
        
        # Assign sequential FAISS ID and add
        faiss_id = next_id
        index.add(embedding)
        next_id += 1
        
        faiss.write_index(index, INDEX_FILE)
        print(f"[INFO] Text added to FAISS with ID {faiss_id}. Total entries: {index.ntotal}")
        
        # Return FAISS ID and normalized text for verification
        confirmation = f"added:{faiss_id}:{normalized_text}"
        client.send_message(CONFIRM_ADDRESS, confirmation)
        
    except Exception as e:
        msg = f"Failed to add text: {e}"
        print(f"[ERROR] {msg}")
        client.send_message(CONFIRM_ADDRESS, msg)

def handle_delete(_: str, *args: List[str]) -> None:
    """Delete command removed - use MongoDB soft delete instead."""
    msg = "/delete not supported. Mark documents as deleted in MongoDB instead."
    print(f"[INFO] {msg}")
    client.send_message(CONFIRM_ADDRESS, msg)

def handle_list_ids(_: str, *args: List[str]) -> None:
    """List all FAISS IDs in the index."""
    if index.ntotal == 0:
        print("[INFO] Index is empty.")
        client.send_message(IDS_ADDRESS, [])
        return
    
    # Sequential IDs from 0 to ntotal-1
    id_list = list(range(index.ntotal))
    
    print(f"[INFO] Listing {len(id_list)} FAISS IDs: 0 to {index.ntotal-1}")
    client.send_message(IDS_ADDRESS, id_list)

def handle_stop(_: str, *args: List[str]) -> None:
    """Stop the server gracefully."""
    print("[INFO] Received /stop command, shutting down server...")
    client.send_message(CONFIRM_ADDRESS, "stopping")
    
    # Save the index one final time
    try:
        faiss.write_index(index, INDEX_FILE)
        print("[INFO] Index saved successfully")
    except Exception as e:
        print(f"[WARN] Failed to save index on stop: {e}")
    
    # Exit gracefully
    sys.exit(0)

# ─── Server setup and graceful shutdown ────────────────────────────────────

def main() -> None:
    setup()

    dispatcher = Dispatcher()
    dispatcher.map(IN_TEXT_ADDRESS, handle_text)
    dispatcher.map(IN_ADD_ADDRESS, handle_add)
    dispatcher.map(IN_STOP_ADDRESS, handle_stop)
    dispatcher.map(IN_LIST_IDS_ADDRESS, handle_list_ids)

    print(f"[INFO] Starting server on {LISTEN_IP}:{LISTEN_PORT}")
    server = BlockingOSCUDPServer((LISTEN_IP, LISTEN_PORT), dispatcher)
    print(f"[READY] Listening on {LISTEN_IP}:{LISTEN_PORT}")
    print(f"        Commands: /text, /add, /stop, /list_ids → replies to {SEND_IP}:{SEND_PORT}")
    print(f"        /add returns FAISS ID via /confirm for storing in MongoDB")
    print(f"        For deletions: mark as deleted in MongoDB, don't remove from FAISS")
    print(f"        Use /stop for graceful shutdown")
    client.send_message(CONFIRM_ADDRESS, "ready")

    def shutdown(sig: int, _frame):
        print("\n[INFO] Shutting down …")
        server.server_close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server.serve_forever()


if __name__ == "__main__":
    main()