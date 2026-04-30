# News Pipeline — Tables & Flow

> Body backfill is owned by a **separate, independent job** (out of scope of this doc). This pipeline assumes `news.news_text` may be `NULL` and fetches on demand when needed.

## Tables

### `news` — one row per `story_id`
| Column | Purpose |
|---|---|
| `story_id` (PK) | Eikon id |
| `heading` | headline text |
| `news_text` | full body — populated opportunistically by the theme job, or by the external backfill |
| `ric`, `source_code`, `version_created` | metadata |
| `theme_status` | `pending` / `excluded_source` / `excluded_ticker` / `duplicate` / `kept_unused` / `kept_used` / `error` |
| `theme_status_at` | when status last changed |
| `kept_story_id` | for duplicates → points to survivor |

### `news_themes` — one row per LLM-extracted theme
| Column | Purpose |
|---|---|
| `theme_id` (PK) | surrogate id |
| `ric`, `ticker`, `company_name`, `date` | grouping keys |
| `theme_name`, `theme_details` | LLM output |
| `positive`, `negative`, `neutral` | sentiment probs |
| `run_id` | which run produced it (optional) |

### `news_theme_sources` — provenance ledger
| Column | Purpose |
|---|---|
| `theme_id` (FK) | the theme |
| `story_id` (FK) | a contributing story |
| `role` | `llm_cited` or `duplicate_of_kept` |
| PK | `(theme_id, story_id)` |

### `news_processing_runs` *(optional)* — audit log
`run_id`, `started_at`, `finished_at`, `start_date`, `end_date`, `status`, `metrics_json`.

---

## Flow

Two daily jobs over the `news` table.

```
┌─────────────────────┐         ┌──────────────────────┐
│ 1. Headline         │         │ 2. Theme extraction  │
│    warehousing      │   ──▶   │    (process_news)    │
└─────────────────────┘         └──────────────────────┘
       writes:                          writes:
   news (heading,                  news.theme_status,
   theme_status=pending,           news.news_text (opportunistic),
   news_text=NULL)                 news_themes,
                                   news_theme_sources
```

> External job (separately scheduled): a body backfill that fills `news.news_text` for older / missed rows. This pipeline does not orchestrate it.

**Step 1 — Warehousing**
`get_news_headlines` → upsert into `news` with `theme_status='pending'`, `news_text=NULL`.

**Step 2 — Themes**
For rows where `theme_status IN ('pending','error')`:
1. Bulk-mark `excluded_source` (blacklisted source codes).
2. Group by `(ric, date)`. Skip + mark `excluded_ticker` for unknown RIC / BSE.
3. Embed headlines, cosine-cluster → mark losers `duplicate` with `kept_story_id`.
4. Build LLM input from survivors:
   - if `news_text` is present in the DB → use it;
   - else call `ek.get_news_story`, write the result back to `news.news_text` opportunistically, and use it for the prompt.
5. LLM returns themes + `source_story_ids`.
6. Insert into `news_themes`, expand cited ids via cluster map, insert into `news_theme_sources`.
7. Survivors → `kept_used` (cited) or `kept_unused` (not cited).
8. Exceptions → `error` (retried next run).

---

## Story lifecycle

```
pending ──▶ excluded_source        (terminal)
        ──▶ excluded_ticker        (terminal)
        ──▶ duplicate              (terminal, kept_story_id set)
        ──▶ kept_unused            (terminal, sent to LLM, not cited)
        ──▶ kept_used              (terminal, in news_theme_sources)
        ──▶ error  ──▶ retried next run
```

Every status except `pending` / `error` is terminal → re-runs are cheap and idempotent.

---

## Useful queries

```sql
-- backlog
SELECT theme_status, COUNT(*) FROM news
WHERE version_created > NOW() - INTERVAL '7 days'
GROUP BY 1;

-- stories behind a theme
SELECT n.story_id, n.heading, n.news_text, s.role
FROM news_theme_sources s JOIN news n USING (story_id)
WHERE s.theme_id = :theme_id;

-- themes a story contributed to
SELECT t.* FROM news_theme_sources s
JOIN news_themes t USING (theme_id)
WHERE s.story_id = :story_id;
```
