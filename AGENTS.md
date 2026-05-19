# Project Rules

## Server AI Output Must Not Be Truncated

- User-visible AI output fields, including `aiSummary`, `titleZh`, `discussionThemes`, `insights`, `terms`, and insights briefs, must not be silently truncated, sliced, or clipped by server validators, persistence, projection, sync, or fallback paths.
- `max_output_tokens` is an output budget signal for planning, batching, prompt guidance, retry, or explicit failure. It must not be used as permission to cut already generated reader-facing text.
- When an AI request has a known output cap, the server prompt should derive reasonable per-story targets from that cap, such as summary length and comment/theme counts, before generation.
- If content is too large for a downstream transport or storage limit, split it into smaller batches/chunks or fail loudly with a clear error. Do not preserve availability by shortening reader-facing content.
- Limits on non-reader diagnostics, IDs, hashes, database query counts, or third-party source input sampling are allowed only as explicit operational/context controls. They must never be applied to already generated reader-facing AI fields.
