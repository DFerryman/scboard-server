"""
hnreader server data contract (slimmed-down version).

After the P4 partial migration the mini-program calls cloud functions
directly, the VPS no longer serves read routes, and the API response models
are no longer needed. Only the few that are *still used by the server
process* remain here:

- ``Story`` + its substructures (``DiscussionTheme`` / ``Insight`` / ``Term`` / ``StoryType``):
  used by ``cloud_sync.build_read_model`` when converting SQLite into the
  cloud-database read model; ``ai_agent`` / ``repository`` also persist data
  in this shape.
- ``TopicEntry``: the topic/category description shared by ``ai_agent`` / ``topics`` / ``repository``.
- ``ErrorResponse``: still used by the admin endpoint error handler in ``server/main.py``.
"""

from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


# ---------- Base enums ----------

class StoryType(str, Enum):
    TOP = "top"
    NEW = "new"
    BEST = "best"
    ASK = "ask"
    SHOW = "show"
    JOB = "job"


# ---------- Story substructures ----------

class DiscussionTheme(BaseModel):
    """A discussion theme that recurs across the top comments."""
    title: str = Field(description="Short Chinese theme name")
    summary: str = Field(description="One-sentence statement of the discussion focus under this theme")


class Insight(BaseModel):
    """A representative comment after AI re-ranking."""
    author: str
    score: int = Field(ge=0)
    text: str


class Term(BaseModel):
    """Term explanation."""
    term: str
    def_: str = Field(alias="def", description="Definition; the serialized field name is def")

    model_config = {"populate_by_name": True}


# ---------- Story main structure ----------

class Story(BaseModel):
    """
    The Story structure actually consumed by the client.
    Backend constraint: every field must be present; empty values use "", []
    or null (according to the types below).
    Omitting a key is forbidden -- the client wxml template does not tolerate undefined.
    """
    id: int
    type: StoryType
    titleZh: str = Field(description="AI-translated Chinese title; may be an empty string")
    titleEn: str = Field(description="Original English title; may be an empty string")
    url: str = Field(description="Original article link; empty string for the Ask HN type")
    domain: str = Field(description="Source domain; filled with news.ycombinator.com when there is no url")
    by: str = Field(description="HN author")
    score: int = Field(ge=0, description="HN score; 0 for the job type")
    descendants: int = Field(ge=0, description="HN comment count; 0 for the job type")
    time: int = Field(description="Actual time the event occurred, unix seconds")
    updatedAt: Optional[int] = Field(default=None, description="Last update time of the AI-consolidated output, unix seconds; may be empty")
    topic: str = Field(description="Topic/category id; must appear in the category catalog")

    # The following are AI-consolidated fields: on failure / when not yet
    # available use null or an empty array, but the key must be present
    aiSummary: str = Field(description="AI-consolidated summary; empty string on failure / when not yet available")
    discussionThemes: List[DiscussionTheme] = Field(
        default_factory=list, description="Comment discussion themes; may be an empty array"
    )
    insights: List[Insight] = Field(
        default_factory=list, description="Representative comments; may be an empty array"
    )
    terms: List[Term] = Field(default_factory=list, description="Key terms; may be an empty array")


# ---------- Categories ----------

class TopicEntry(BaseModel):
    id: str
    name: str
    count: int = Field(ge=0)


# ---------- Error envelope (admin route error handler) ----------

class ErrorResponse(BaseModel):
    """Unified format for non-2xx responses."""
    code: Literal[
        "BAD_REQUEST",
        "UNAUTHORIZED",
        "NOT_FOUND",
        "RATE_LIMIT",
        "SERVICE_UNAVAILABLE",
        "INTERNAL",
    ]
    message: str
