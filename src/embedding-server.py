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
  • /add "type" "text" "{json_metadata}"
  • Returns via /matches: [type1, text1, score1, type2, text2, score2, ...]
  • Confirmation via /confirm: "added" or error string
"""
from __future__ import annotations

import signal
import sys
import json
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
OUT_ADDRESS = "/matches"
CONFIRM_ADDRESS = "/confirm"

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 5
INDEX_FILE = "unified.index"
ENTRY_FILE = "entries.json"

# ─── Load entry metadata ────────────────────────────────────────────────────
def load_entries() -> List[dict]:
    if os.path.exists(ENTRY_FILE):
        with open(ENTRY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_entries(entries: List[dict]) -> None:
    with open(ENTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)

# ─── Initialise model, FAISS index, and OSC client ─────────────────────────
try:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer(MODEL_NAME, device=device)
    print(f"[INIT] Embedding model loaded on: {device.upper()}")
except Exception as e:
    print(f"[WARN] Failed to load model on GPU, falling back to CPU: {e}")
    device = "cpu"
    model = SentenceTransformer(MODEL_NAME, device=device)
    print("[INIT] Embedding model loaded on: CPU")

client = SimpleUDPClient(SEND_IP, SEND_PORT)

entries: List[dict] = load_entries()

if os.path.exists(INDEX_FILE):
    index = faiss.read_index(INDEX_FILE)
    faiss_device = "CPU (loaded from disk)"
else:
    index = faiss.IndexFlatIP(384)
    faiss_device = "CPU (new)"
    faiss.write_index(index, INDEX_FILE)

print(f"[INIT] FAISS index initialized on: {faiss_device}")

# ─── OSC Callbacks ─────────────────────────────────────────────────────────

def handle_text(_: str, *args: List[str]) -> None:
    """Process a /text query: lower‑case input, embed, search, return kebab‑case matches."""
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
    for rank, idx in enumerate(indices[0]):
        entry = entries[idx]
        flat.extend([
            entry["type"],
            entry["text"].replace(" ", "-"),  # kebab‑case
            float(scores[0][rank]),
        ])

    client.send_message(OUT_ADDRESS, flat)
    print(f"[INFO] Sent {len(flat)//3} matches.")

def handle_add(_: str, *args: List[str]) -> None:
    """Add or replace an entry. Always store text in lower‑case."""
    global index
    if len(args) < 2:
        msg = "/add requires at least type and text."
        print(f"[WARN] {msg}")
        client.send_message(CONFIRM_ADDRESS, msg)
        return

    entry_type = str(args[0]).lower().strip()
    raw_text = str(args[1])
    text = " ".join(raw_text.lower().split())  # canonical lower‑case

    try:
        metadata = json.loads(args[2]) if len(args) > 2 else {}
    except json.JSONDecodeError:
        metadata = {}
        print("[WARN] Invalid JSON metadata, using empty dict.")

    print(f"[INFO] Adding: ({entry_type}) {text!r}")

    # Remove duplicates of same type & text
    entries[:] = [e for e in entries if not (e["text"] == text and e["type"] == entry_type)]

    embedding = model.encode(text, normalize_embeddings=True).reshape(1, -1)
    entry = {"text": text, "type": entry_type, "metadata": metadata}
    entries.append(entry)
    save_entries(entries)

    # Rebuild FAISS index on CPU
    cpu_index = faiss.IndexFlatIP(384)
    all_embeddings = model.encode([e["text"] for e in entries], normalize_embeddings=True)
    cpu_index.add(np.asarray(all_embeddings, dtype=np.float32))
    faiss.write_index(cpu_index, INDEX_FILE)
    index = cpu_index

    print("[INFO] FAISS index rebuilt on CPU.")
    print(f"[INFO] Entry added and saved. Total: {len(entries)}")
    client.send_message(CONFIRM_ADDRESS, "added")



# ─── Server setup and graceful shutdown ────────────────────────────────────

def main() -> None:
    dispatcher = Dispatcher()
    dispatcher.map(IN_TEXT_ADDRESS, handle_text)
    dispatcher.map(IN_ADD_ADDRESS, handle_add)

    server = BlockingOSCUDPServer((LISTEN_IP, LISTEN_PORT), dispatcher)

    print(
        f"[READY] Listening on {LISTEN_IP}:{LISTEN_PORT} /text + /add → "
        f"{SEND_IP}:{SEND_PORT} /matches"
    )

    def shutdown(sig: int, _frame):
        print("\n[INFO] Shutting down …")
        server.server_close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    server.serve_forever()


if __name__ == "__main__":
    main()
