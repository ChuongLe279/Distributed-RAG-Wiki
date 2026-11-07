from typing import Any

import numpy as np


def encode_question(question: str, encoder: Any) -> np.ndarray:
    """Encode a normalized E5 query vector."""
    if not isinstance(question, str) or not question.strip():
        raise ValueError("A nonempty question is required.")
    question = question.strip()
    query = encoder.encode(
        f"query: {question}", normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False,
    )
    query = np.asarray(query, dtype=np.float32)
    if query.shape != (768,) or not np.isfinite(query).all() or not np.isclose(np.linalg.norm(query), 1.0, atol=1e-3):
        raise ValueError("The E5 query encoder must return a normalized finite vector of length 768.")
    return query
