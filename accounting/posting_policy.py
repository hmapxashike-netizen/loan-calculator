"""Shared GL posting-policy context."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Literal

PostingPolicy = Literal["standard", "eod_replay"]

_GL_POSTING_POLICY: ContextVar[PostingPolicy] = ContextVar(
    "gl_posting_policy",
    default="standard",
)


def get_gl_posting_policy() -> PostingPolicy:
    """Current posting policy for this execution context."""
    return _GL_POSTING_POLICY.get()


@contextmanager
def use_gl_posting_policy(policy: PostingPolicy) -> Iterator[None]:
    """Temporarily override posting policy for the active context."""
    tok = _GL_POSTING_POLICY.set(policy)
    try:
        yield
    finally:
        _GL_POSTING_POLICY.reset(tok)
