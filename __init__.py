"""hnreader server package — HN content backend, frontend-agnostic.

Sync-only: ingest writes SQLite, cloud_sync pushes a read model + dashboard
projection to the cloud DB; no HTTP listener runs on this host.

Layout:

- ``schemas``     Pydantic data contract shared with all frontend clients.
- ``settings``    Runtime configuration knobs.
- ``db``          SQLite connection management and DDL.
- ``repository``  Row-to-Story conversion and data access helpers.
- ``topics``      Fixed topic catalog (English IDs, Chinese display names).
- ``hn_client``   Hacker News HTTP client.
- ``normalizer``  HN item -> stories row normalization.
- ``ai_agent``    Fallback and provider-backed AI enrichment agents.
- ``ingest``      Fetcher + Enricher + scheduler CLI entry point.
- ``digest``      Daily digest generator.
- ``cleanup``     Retention and eviction job.
- ``cloud_sync``, ``cloud_push``, ``cloud_sync_runner``,
  ``dashboard_projection``   Read-model build + HMAC push to the cloud DB.
"""
