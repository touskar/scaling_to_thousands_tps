# Designing a Payment Platform That Handles More Daily Transactions Than Stripe and PayPal

### How a single-writer Go + PostgreSQL gateway reached a measured 5,337 pay-in TPS — and what every wall along the way actually taught us

**Moussa Ndour — Infrastructure Engineer**

---

> **Abstract.** I had to build a **mobile-money payment gateway** — one that
> integrates with PSPs (Payment Service Providers / mobile-money operators) — and
> handles the maximum possible
> transactions per second. This article is the engineering story of how I got
> there: the architecture I chose up front (a hot/cold partitioned data model, a
> change-data-capture pipeline to OpenSearch, and an append-only money ledger),
> and the ten-or-so walls I hit afterward — each one diagnosed by measurement,
> several of them misdiagnosed first. On a **single** Aurora PostgreSQL writer the
> platform now sustains **5,337 pay-in TPS** and **4,150 pay-out TPS**, with reads
> and payment-request creation exceeding **10,000 TPS**. At those rates the daily
> capacity of the write path alone is several times Stripe's record day. Every
> number below is real, measured on staging, and reported honestly — including the
> guesses that turned out wrong, because those are the parts worth reading.

---

## Table of contents

1. [The challenge](#1-the-challenge)
2. [What this system actually does](#2-what-this-system-actually-does)
   - [2.1 What a payment gateway is](#21-what-a-payment-gateway-is)
   - [2.2 The core operations](#22-the-core-operations)
   - [2.3 How a pay-in / pay-out actually works](#23-how-a-pay-in--pay-out-actually-works)
   - [2.4 The three dispatch modes](#24-the-three-dispatch-modes)
3. [The results](#3-the-results)
4. [Architecture foundations](#4-architecture-foundations)
   - [4.1 The hot/cold data model: daily partitioning](#41-the-hotcold-data-model-daily-partitioning)
   - [4.2 Change data capture to OpenSearch](#42-change-data-capture-to-opensearch)
   - [4.3 The append-only ledger](#43-the-append-only-ledger)
   - [4.4 What the schema looks like](#44-what-the-schema-looks-like)
   - [4.5 The periodic jobs (crontab)](#45-the-periodic-jobs-crontab)
5. [The journey](#5-the-journey)
   - [J1 — The hot wallet (the original wall)](#j1--the-hot-wallet-the-original-wall)
   - [J2 — FK FOR KEY SHARE → MultiXact SLRU contention](#j2--fk-for-key-share--multixact-slru-contention)
   - [J3 — The misdiagnosed "720 commit wall"](#j3--the-misdiagnosed-720-commit-wall)
   - [J4 — The RDS Proxy saga: starved, not capped](#j4--the-rds-proxy-saga-starved-not-capped)
   - [J5 — "The database is not the bottleneck"](#j5--the-database-is-not-the-bottleneck)
   - [J6 — The real single-writer wall: monotonic keys + one WAL](#j6--the-real-single-writer-wall-monotonic-keys--one-wal)
   - [J7 — The unified transaction-write batcher](#j7--the-unified-transaction-write-batcher)
   - [J8 — The Asynq Redis self-DDoS and the io_multiplier trap](#j8--the-asynq-redis-self-ddos-and-the-io_multiplier-trap)
   - [J9 — The honest ceiling: the test harness itself](#j9--the-honest-ceiling-the-test-harness-itself)
6. [The configuration that got us there](#6-the-configuration-that-got-us-there)
   - [6.1 Full configuration reference](#61-full-configuration-reference)
   - [6.2 Compute and instance sizing](#62-compute-and-instance-sizing)
   - [6.3 Key file paths](#63-key-file-paths)
7. [Honest limits and what's next](#7-honest-limits-and-whats-next)
8. [Principles](#8-principles)

---

## 1. The challenge

Let me set the stakes with the numbers the industry publishes.

- **PayPal** processes on the order of **70–80 million** transactions per day.
- **Stripe** is in the same league, and its single busiest day on record handled
  about **116 million** transactions.
- **Mastercard**, a card network rather than a gateway, runs around **500 million**
  transactions per day.

These are the reference points for "a lot." When I started, the brief was blunt:
build a payment gateway for West Africa — pay-in, pay-out, wallets, payment links,
merchant payments — that handles the **maximum possible** throughput on
infrastructure we can actually afford and operate. Not a benchmark toy: a real
system of record, money-safe, auditable, with a back office, that a regulator or
an acquirer would sign off on.

The platform is a mobile-money payment gateway that integrates with PSPs, built as
roughly thirty Go binaries on PostgreSQL/Aurora,
Redis, OpenSearch, and Asynq (a Redis-backed job queue). The interesting part for
this article is the **write hot path**: `POST /api/v1/payin` and
`POST /api/v1/payout`. A payment write is the hardest thing to scale because it
touches money — it must be atomic, durable, never double-spent, never lost.

A naïve wallet design caps at **tens of TPS** the moment a single popular wallet
gets hot. The story below is how that became thousands.

---

## 2. What this system actually does

Before the numbers, a plain-language map of the territory — because the rest of the
article only makes sense if you know what a "pay-in" is and why the "settle" step is
the hard part. If you live in payments, skim ahead to [§3](#3-the-results). If you
don't, ten minutes here will repay themselves.

### 2.1 What a payment gateway is

A payment gateway sits **between a merchant and the money rails.** On one side is the
merchant — an e-commerce site, a mobile app, a biller — who just wants to charge a
customer or pay someone out. On the other side are the **Payment Service Providers**
(PSPs): in West Africa these are the mobile-money operators — **Wave, Orange Money,
MTN MoMo, Moov Money** — each with its own API, its own auth, its own quirks, its own
fee schedule, its own way of telling you a payment succeeded.

Without a gateway, the merchant has to integrate every operator separately: four sets
of credentials, four signature schemes, four callback formats, four reconciliation
processes. The gateway collapses all of that into **one API**. The merchant calls
`POST /api/v1/payin` once; the gateway handles the rest — authenticating the caller,
computing fees, **routing to the right PSP**, moving the money, recording it in a
ledger, delivering a webhook when it's done, and reconciling what the PSP says
happened against what we recorded. The merchant never touches an operator directly.

Think of it as the **adapter and the system of record at the same time**: an adapter
because it speaks every operator's protocol behind one clean interface, and a system
of record because *we* are the source of truth for who is owed what — the ledger,
the audit trail, the balances a regulator or an acquirer will eventually inspect.

### 2.2 The core operations

Everything the platform does is a variation on a handful of primitives:

- **Pay-in** — *collect* money from a customer through a PSP. A merchant charges a
  customer; the customer approves on their phone (Wave, Orange Money…); the funds
  land and the merchant's wallet is credited.
- **Pay-out** — *send* money to someone through a PSP. The merchant instructs a
  disbursement; we debit their wallet and tell the PSP to push funds to the
  recipient's mobile-money account.
- **Payment links / payment requests** — a hosted page or a generated request that
  lets someone pay without the merchant writing any integration code at all. (These
  are cheap to create — pure inserts — which is why their TPS is so high.)
- **Transaction status** — "what happened to transaction X?" A read, because
  mobile-money is asynchronous: the answer can arrive seconds after the request.
- **Refunds** — give a collected payment back, which reverses the ledger movements.
- **The wallet / ledger** — the running record of every balance and every movement.
  This is the heart of the system of record, and (as the whole second half of this
  article shows) the hardest thing to make fast, because money must never be
  double-counted, lost, or overdrawn.

### 2.3 How a pay-in / pay-out actually works

The single most important thing to understand about mobile-money is that **it is
asynchronous.** You don't call the PSP and get "success" back on the same line. You
ask the PSP to start a payment, it says "OK, I'm working on it," and **later** —
after the customer taps approve, after the operator's own systems clear it — the PSP
calls *you* back to say how it ended. That two-beat rhythm shapes the entire
architecture.

A pay-in (pay-out is the mirror image) walks through these steps:

1. **Receive & authenticate.** The merchant calls our one API with their API key.
   We validate the key, the signature, and the request.
2. **Price & check.** We compute the fees and, for a debit, check the wallet has the
   funds (the overdraft guard).
3. **Create the transaction — `PENDING`.** We write a transaction row to PostgreSQL
   in state `PENDING`. This is now a durable fact: we have promised to attempt it.
4. **Call the PSP (HTTP).** We send the request to Wave / Orange / MTN / Moov over
   their API. They acknowledge and start processing on **their** side.
5. **…time passes.** The customer approves on their phone; the operator clears it.
   Seconds, sometimes longer.
6. **The PSP sends a callback.** The operator makes an HTTP call **back to us** with
   the final outcome — success or failure. This is the beat everything waits for.
7. **Settle.** On the callback we **settle** the transaction: flip its status, and —
   the money part — append the ledger movements (credit the merchant on a pay-in,
   confirm the debit on a pay-out).
8. **Deliver the webhook.** We notify the merchant's own server that the transaction
   reached its final state, so their system can react.

Two steps are the heart of it, and the two that dominate this article: **the PSP call
(step 4)** and **the asynchronous callback that triggers settle (steps 6–7).** The
call is I/O-bound — a network round-trip to an external operator we don't control.
The settle is DB-bound — it touches the money ledger and must be exactly-once. Those
two workloads pull in opposite directions, and reconciling them is a recurring theme
below (notably in [J8](#j8--the-asynq-redis-self-ddos-and-the-io_multiplier-trap)).

### 2.4 The three dispatch modes

Because the PSP call is slow and asynchronous, there's a genuine design choice in
**how long the merchant's HTTP request waits.** The platform supports all three
answers, switchable platform-wide via a single config flag, `dispatch.global_mode`.
The trade-off is always the same — caller simplicity versus throughput — and it's
worth understanding precisely, because the headline TPS numbers depend on which mode
is in play.

- **Synchronous.** The API handler holds the HTTP connection open, calls the PSP
  **inline**, and waits for the result before responding. The merchant gets the final
  answer in a single call — simplest possible integration. But the request thread is
  **tied up for the entire PSP round-trip**, which on mobile-money can be a second or
  more. At load this is the worst mode: every in-flight payment is also an in-flight
  held connection, and you run out of threads long before you run out of database.

- **Semi-async.** A compromise. The handler waits **up to a bounded timeout** (a few
  seconds) for the settle to complete; if the PSP answers in time, the caller gets the
  final status synchronously, and if it doesn't, the handler returns a `PENDING` and
  lets the **callback settle it later**. You get the convenience of an inline answer
  when the PSP is quick, without paying the unbounded-wait cost when it's slow. A
  latency-versus-throughput middle ground.

- **Asynchronous.** The handler does the minimum durable work — authenticate, price,
  create the `PENDING` transaction — **enqueues the provider call as a background
  job**, and returns immediately (`202` / `PENDING`). A worker picks up the job, calls
  the PSP, and the callback settles the transaction later. The request thread is
  **freed instantly**, so the API can accept new work at the rate it can *write
  transactions*, not the rate the PSP can *answer*. This is the mode behind the
  high-TPS numbers in this article.

The trade-off is honest and unavoidable: **async maximizes throughput by decoupling
"accepted" from "settled,"** but it pushes a responsibility onto the merchant — they
must handle the **webhook** (or poll the status endpoint) to learn the final outcome,
because the initial response only says "accepted," not "succeeded." Synchronous hands
the merchant a finished answer at the cost of throughput; asynchronous hands them
throughput at the cost of having to listen for the result. Most of this article lives
in the asynchronous world, because that's where the interesting walls are.

---

## 3. The results

All numbers are **measured on staging** (AWS eu-west-3), driven from a bastion
inside the VPC running `hey` so the client→Cloudflare→ALB internet path never
pollutes the measurement. "Multi-wallet" means load spread across ~30 wallets,
which is the realistic production shape; single-wallet is the worst case and is
called out where relevant. These are **end-to-end** numbers — the settle pipeline
keeps up, not init-only vanity figures (the difference cost me a chapter; see
[J8](#j8--the-asynq-redis-self-ddos-and-the-io_multiplier-trap)).

| Endpoint | Sustained TPS (measured) | Daily capacity (×86,400) | avg latency (moderate load) | p99 (slowest 1% — max) |
|---|---:|---:|---:|---:|
| **Pay-In** | **5,337** | **~461 M / day** | ~0.6 s | ~5.5 s (at peak/saturation) |
| **Pay-Out** | **4,150** | **~358 M / day** | ~0.5 s | ~2.0 s (at peak) |
| **Create Payment Request** | **10,156** | **~877 M / day** | ~0.26 s | ~0.8 s |
| **Check Transaction (status)** | **10,957** | **~947 M / day** | ~0.15 s | ~0.9 s |
| **Get Payment Link** | **11,237** | **~971 M / day** | ~0.14 s | ~0.9 s |

### 3.1 Reading the latency columns: what avg and p99 actually mean

Two latency numbers are reported per path, and they answer two different questions.

- **avg (average)** is the mean response time across *all* requests — the time a
  typical request takes. For pay-in at the saturation peak that's **~0.6 s**.
- **p99 (99th percentile)** is the value below which **99% of requests completed** —
  only the slowest **1%** took longer. So **p99 = 5.5 s does *not* mean 99% of
  requests took 5.5 s.** It means **99% of requests finished *faster* than 5.5 s,
  and only the slowest 1% exceeded it.** The typical request is the avg (~0.6 s);
  the p99 captures the worst-case *tail*, not the common case.

A quick mental model: line up every request from fastest to slowest. The average is
roughly the middle of the pile; the p99 is the request sitting at the 99% mark — far
out in the slow tail, with just 1% of requests behind it.

**What "the 1%" actually is, in concrete numbers (approximate).** The scale makes
the tail feel much smaller than the word "5.5 s" suggests. The headline pay-in run
pushed **~5,337 TPS for ~30 seconds**, which is roughly **~160,000 requests** in
that window. So p99 = 5.5 s means:

- the **slowest ~1% ≈ ~1,600 requests** took longer than 5.5 s, while
- the **other ~99% (~158,400 requests) completed in under 5.5 s**, and
- the **typical/average request was ~0.6 s**.

In other words, 5.5 s is the worst-case tail of a *tiny minority* of requests, hit
only at peak saturation — **not** the common experience. The overwhelming majority
finished sub-second; the 5.5 s figure is what the unluckiest ~1,600-or-so callers
saw while the single writer was pinned at its absolute ceiling.

**Why report p99 and not only the average?** An average can hide a nasty tail: a
system can look fast on average while a small slice of users wait a long time. The
p99 is what SRE practice and SLAs actually care about, because it describes the
*worst* experience a real user is likely to hit, not the comfortable middle.

**What the numbers say, stated honestly.** The reads and create-request paths are
**stable sub-second** — even their p99 stays under 1 s, with 0 errors, at the full
TPS shown. The two write paths behave differently, and that difference is the whole
point of the headline numbers: pay-in/pay-out latency is **low at moderate load**
(avg ~0.5–0.6 s), but at the **saturation peak** the slowest 1% of pay-in requests
reach **~5.5 s** — the p99 tail — because requests queue in front of the single
writer at maximum throughput. The headline 5,337 / 4,150 TPS are the **maximum
capacity**, measured precisely at that writer-saturation point. At a normal operating
point **below the knee** (~3,000–4,000 pay-in TPS) even the p99 stays **~1–2 s**.
This is expected single-writer behaviour: as the one writer saturates, throughput
peaks *and* the latency tail grows — you trade tail latency for the last increment of
throughput. The honest figure to plan against is the below-the-knee operating point,
not the saturation peak.

**The pay-in vs pay-out p99 "paradox" — why it is not incoherent.** A careful reader
will spot something that looks contradictory in the table: **pay-in (5,337 TPS, p99
5.5 s) shows a *worse* p99 than pay-out (4,150 TPS, p99 2.0 s)** — even though pay-out
does *more* work per request. Pay-out takes a **per-wallet advisory lock and runs an
overdraft check** on every debit; pay-in (a credit) is a **lock-free append** that
contends with nothing ([§4.3](#43-the-append-only-ledger)). So how can the cheaper,
lock-free path have the longer tail? This is **not** a sign that pay-in is "slower,"
and it is **not** incoherent. It is the single clearest illustration in the whole
campaign of what tail latency actually measures.

The key idea: **p99 (tail latency) is driven by how hard you push a path relative to
*its own* capacity ceiling — not by the per-request work.** As any queueing system
approaches 100% utilization, a queue forms and the tail blows up; the closer to the
ceiling, the longer the tail. The per-request cost sets *where* the ceiling is; how
close you drive to it sets *how long the tail gets*.

- **Pay-in** was driven all the way to its **absolute saturation peak** — 5,337 TPS
  *is* its maximum. At 100% utilization a queue forms in front of the single writer,
  so the slowest 1% tail inflates to **5.5 s**. The measurement was taken precisely
  at the breaking point, which is exactly where the tail is worst.
- **Pay-out's** ceiling is **lower** (~4,150 TPS, capped by the per-wallet debit
  lock), and the measured run **did not push it as deep into its own queue** — so its
  tail stayed shorter, at **2.0 s**. Less work per request gives it a *lower* ceiling,
  but it was simply measured at a less-saturated point relative to that ceiling.

**The proof that it's saturation, not per-request cost.** At a comparable **moderate
load below the knee** (~3,000–4,000 TPS), pay-in's p99 would also be **~1–2 s** — the
*same ballpark* as pay-out. Conversely, if you pushed pay-out to *its* exact breaking
point, *its* tail would inflate too. The two numbers in the table (5.5 s vs 2.0 s)
were measured at **different saturation depths**, so comparing them head-to-head is
**apples-to-oranges**. At equal moderate load, both paths land at roughly **~0.5 s
avg / ~1–2 s p99**.

So the rule to carry away is: **p99 ≈ f(how close to capacity you push), not f(work
per request).** And the pay-out angle is the proof, worth insisting on:
**pay-out does *strictly more work* per request — an advisory lock plus an overdraft
check — yet shows the *lower* p99.** If tail latency tracked per-request cost, pay-out
would have the worse tail. It has the better one. That inversion is precisely the
evidence that the tail is about **saturation and queueing**, not about how expensive
each individual request is.

> Note: this campaign captured **avg and p99 only** — p95 was not measured
> separately — so the table reports those two.

Put next to the industry reference points:

- The **write** paths — the genuinely hard ones — sit at **358–461 M/day**, which
  is roughly **3–4× Stripe's record day** (116 M) and **~5–6×** PayPal's daily
  volume.
- The **read** and **create-request** paths (**877–971 M/day**) approach or exceed
  **Mastercard's daily volume** (~500 M).

The honest caveat, stated up front and unpacked at the end: these are
**single-Aurora-writer** numbers. One PostgreSQL writer cannot do 10,000 *write*
TPS for this insert pattern — that requires sharding. But it can do ~5,000, and at
~5,000 the daily capacity already dwarfs the references. Reads and create-request
already clear 10,000 on the single writer because they don't fight over the write
hotspots.

The rest of this article is how each of these numbers was earned.

---

## 4. Architecture foundations

Three design decisions made everything afterward possible. None of them is a
performance "trick" — they are the shape of the system. I'll explain each so a
reader who has never seen the codebase understands *why* it's built this way.

### 4.1 The hot/cold data model: daily partitioning

At 3,000 TPS the platform produces roughly **260 million transaction rows per day**
and **~520 million ledger rows per day** (two ledger movements per transaction). No
single store is simultaneously a fast OLTP system of record *and* a cheap
long-retention analytics store. So data lives in tiers, and PostgreSQL is kept
deliberately small and hot.

**15 high-volume tables are RANGE-partitioned by day on `created_at`**, each with a
one-line purpose below.

*Gateway side (8):*

| Table | Purpose |
|---|---|
| `transactions` | pay-in / pay-out records (the write hot path) |
| `wallet_statements` | gateway-side ledger statements (≈2 per transaction) |
| `provider_statements` | provider balance movements (the system-balance audit trail) |
| `audit_logs` | compliance trail — **never detached** |
| `webhook_deliveries` | merchant webhook attempts + retries |
| `payment_links` | gateway-hosted payment links |
| `wallet_reloads` | cash-in via a PSP |
| `wallet_withdrawals` | cash-out via a PSP |

*Wallet side (7):*

| Table | Purpose |
|---|---|
| `w_transactions` | intra-wallet P2P transfers |
| `w_wallet_statements` | wallet-side ledger statements |
| `w_payment_requests` | P2P payment requests |
| `w_wallet_reloads` | wallet-side reload alias |
| `w_wallet_withdrawals` | wallet-side withdrawal alias |
| `w_merchant_payment_links` | merchant payment intents (wallet side) |
| `w_card_auth_holds` | card authorization holds |

On top of those 15, the **append-only `ledger_entries`** table
([§4.3](#43-the-append-only-ledger)) is partitioned the same way (migration `108`),
and so are the three **observability** tables written for distributed tracing —
`transaction_events` (discrete lifecycle events per transaction),
`transaction_flow_spans` (timed spans for HTTP/dispatch steps), and `callback_audit`
(every inbound provider/partner callback) — added in the observability-foundation
migration (`087`). A short list of high-write tables is deliberately **left
unpartitioned** because they are low-volume or slowly-changing: `w_cards`,
`w_card_disputes`, and `w_card_tokens`.

**Partition lifecycle.** Two daily crons keep the hot store bounded, plus a third,
special-cased cron for the ledger:

- **`partition:create_next`** (daily, 2 a.m.) pre-creates **N + `partition.advance_days`**
  (default **30**) future daily partitions for every partitioned table, so writes
  never arrive at a day that has no child partition.
- **`partition:detach_old`** (daily, 2 a.m.) **detaches** partitions older than
  `partition.detach_after_days` (default **90**) from every table **except**
  `audit_logs`, which is never detached because it is the compliance trail. Detach,
  not drop — detached partitions are reclaimed manually only after the OpenSearch
  archive is verified.
- **`ledger:detach`** — the ledger is **not** detached by the generic cron. It has
  its own **checkpoint-coverage-gated** detach job
  (`internal/gateway/application/jobs/ledger/detach_job.go`), governed by
  `ledger.detach_gated_enabled`. When the flag is **off**, `ledger_entries` is
  **never** auto-detached — the safe default posture, because detaching a ledger
  partition whose balance hasn't been folded and snapshotted would lose money math.
  When the flag is **on**, a partition becomes detachable **only if a `balance_checkpoint`
  watermark covers its last entry id** — i.e. the folded balance for everything in
  that partition has already been snapshotted into `balance_checkpoints`. Partitions
  with no covering checkpoint stay attached; if no checkpoint exists at all, the job
  refuses to detach anything. That is what "checkpoint-coverage-gated detach" means:
  a partition is only ever let go once its balances are provably anchored elsewhere.

With daily granularity, "keep 90 days hot" means 90 partitions, each small enough
that autovacuum handles it trivially.

**Why daily, and why it doesn't slow reads.** A by-id lookup like `WHERE id = ?`
would normally have to scan *every* active partition. Two things prevent that:

1. **All primary keys are UUID v7**, which embeds the creation timestamp in its
   high bits, so IDs are chronologically ordered. A `WithUUIDv7PartitionPruning`
   scope decodes the timestamp from the id and adds a `created_at` range filter, so
   PostgreSQL touches **only the one matching partition**. An id lookup stays O(1)
   regardless of how many days are retained.
2. **Every list endpoint requires a date range** and routes through
   `determineReadSource(date_from, date_to)`: within the 90-day cutoff → PostgreSQL;
   older → OpenSearch; crossing the cutoff → query both and merge. Pagination is
   cursor-based (`(created_at, id)` ordering, never OFFSET) and works seamlessly
   across the PostgreSQL→OpenSearch boundary, because UUID v7 makes that pair a
   total order on both sides.

So PostgreSQL only ever holds the **hot** ~90 days; everything older is served from
OpenSearch. That keeps the writer's working set small, which matters enormously for
the insert-contention story later.

### 4.2 Change data capture to OpenSearch

PostgreSQL is the system of record; **OpenSearch is a derived read replica** fed by
change data capture (CDC). The back office and partner portals do all their list
and analytics reads from OpenSearch (aggregations are computed in OpenSearch, then
UUIDs are resolved to human names via PostgreSQL lookups). This offloads heavy
analytical traffic from the write path entirely.

The pipeline: **AWS DMS** captures every write to an indexed table → streams onto
**Amazon Kinesis** → a **stream consumer** transforms each change event and routes
it to the day's OpenSearch index `<entity>-YYYY.MM.DD`.

The change stream itself comes from **PostgreSQL logical decoding** — every
INSERT/UPDATE on an indexed table is decoded from the WAL into a change event that
DMS lands on Kinesis. From there a single component owns the job of turning that raw
change event into the right OpenSearch document in the right index. That component —
the **stream consumer** — is deliberately **pluggable**: the same logical step
(parse → transform → route to the daily index) can run two ways, and which one you
pick is purely a function of volume.

Two non-obvious decisions are worth recording because they cost real debugging:

- **DMS source plugin = `test_decoding` + `REPLICA IDENTITY FULL` on every indexed
  table.** AWS DMS officially supports only `test_decoding` and `pglogical`.
  `pglogical` does not support `REPLICA IDENTITY FULL` and silently drops UPDATEs on
  TOASTed JSONB columns; `pgoutput` isn't in the support matrix. Only
  `test_decoding + REPLICA IDENTITY FULL` reliably captures UPDATEs on
  unchanged-TOAST tuples while staying inside the supported envelope. This is frozen
  — do not "upgrade" it.
- **The OpenSearch side needs an index template + ingest pipeline to route by
  `created_at`.** DMS writes each row to a flat `<entity>` index. The app *reads*
  daily `<entity>-YYYY.MM.DD` indices. A painless ingest pipeline
  (`daily-suffix`) rewrites `ctx['_index']` to the daily index based on the
  row's `created_at`, wired via an index template's `index.default_pipeline` — the
  only AWS-supported way to apply a default pipeline to a DMS target.

**A real bug from this design, honestly.** A class of writers (crash-recovery and
sync-retry jobs) extracted `created_at` as a string only; entities that emit a
`time.Time` (e.g. `payment_links`) fell through to `time.Now()` and got re-indexed
into *today's* index on every recovery run. The same document then appeared in
several daily indices, and a back-office list spanning those days rendered it
multiple times. The fix was to route **every** ES writer by the row's real
`created_at` (handling string, `time.Time`, and `*time.Time`), plus a one-time
dedup reindex. The general lesson — **any document's index must be a pure function
of its `created_at`, never of wall-clock-now** — recurs throughout this article in
the database too.

**The pluggable consumer (`ingest_type = lambda | flink`).** The two
implementations do the same four steps, at very different scales:

- **AWS Lambda — the low-/moderate-volume default.** A small serverless function is
  triggered per Kinesis batch; for each record it parses the logical-decoding
  payload, maps it to the OpenSearch document shape, computes the daily index name
  from `created_at`, and `_bulk`-writes it. It is **stateless per record**,
  near-zero ops, and scales by Kinesis shard count. At moderate change rates this is
  all you need, and there is nothing to operate between events.

- **Apache Flink — the high-throughput, stateful path.** When the change volume is
  large — **hundreds of millions of rows per day** at 3K TPS — the right tool is a
  true stream-processing engine. Flink runs the CDC routing as a **managed,
  long-lived job on a cluster of parallel operators**, and that buys properties a
  per-invocation Lambda cannot give you:
  - **Parallelism across a cluster** — the change stream is partitioned across many
    operator subtasks, so ingest throughput scales with the cluster, not with a
    single function's concurrency.
  - **Stateful, exactly-once processing** — Flink **checkpoints** operator state, so
    on failure the job resumes from the last checkpoint without re-emitting or
    dropping documents. Combined with idempotent document IDs on the OpenSearch side,
    this gives end-to-end exactly-once ingest even across restarts.
  - **Native backpressure handling** — if OpenSearch's `_bulk` slows down, Flink
    propagates backpressure up the pipeline and throttles the source, instead of
    piling up failed invocations and retries the way a fan-out of Lambdas would.
  - **Windowing / batching** — records can be grouped into efficient `_bulk` batches
    by operator, smoothing the write load on the cluster.

  Each CDC change event flows through the Flink pipeline that **(a)** parses the
  logical-decoding payload, **(b)** transforms and maps it to the OpenSearch document
  shape, **(c)** computes the daily index name `<entity>-YYYY.MM.DD` from the row's
  **real `created_at`** (the same `created_at`-is-destiny routing rule described
  above — the document's index is a pure function of its `created_at`, never of
  wall-clock-now), and **(d)** bulk-routes it to OpenSearch.

The choice is the same right-sizing instinct that runs through this whole article:
**Lambda when the volume is modest, Flink when the change rate is genuinely large
and you need stateful, exactly-once, backpressure-aware stream processing.** Either
way, at 3K+ TPS the real ceiling is OpenSearch's `_bulk` capacity, not the consumer —
so you scale data nodes and shard count first, and reach for Flink's parallelism when
the consumer itself becomes the wall.

### 4.3 The append-only ledger

This is the foundation the entire write-path performance story stands on.

The classic wallet design is a read-modify-write of shared state:

```sql
UPDATE wallets SET balance = balance ± :amount WHERE id = :wallet_id;
```

…plus a statement row stamping `balance_before`/`balance_after` in the same
transaction. It is correct, auditable, and **slow** the instant a single wallet
gets hot — and the float/aggregation wallets of a payment platform (the platform's
own collection wallet, a big merchant's wallet) are exactly the ones that get hot.
Each debit holds the wallet row's write lock across one fsync (the WAL flush at
commit) plus the statement INSERT; the next debit on that wallet cannot even start
its overdraft check until the previous one has flushed and committed. Measured
ceiling on a single hot wallet: **~55 TPS**. That row is the enemy.

The cure is to stop storing the balance as mutable state and instead **store the
movements and derive the balance**. Migration `108_20260623_wallet_ledger.sql`
introduced `ledger_entries`:

- Daily-partitioned, composite PK `(id, created_at)` with `DEFAULT public.uuidv7()`,
  and **append-only enforced by a `BEFORE UPDATE/DELETE` trigger** — the row
  physically cannot be mutated. Columns: `id`, `wallet_id`, `company_id`,
  `transaction_id`, `entry_type`, `amount NUMERIC(20,2)` (**signed**), `currency`,
  `created_at`. **No `state` column and no foreign keys** — deliberately, for
  immutability and insert speed (foreshadowing [J2](#j2--fk-for-key-share--multixact-slru-contention),
  where FKs turn out to be a serious cost).

The balance becomes a **derived value**: `wallets.balance` is now a *folded cache*,
and `wallets.last_batched_entry_id` is a watermark.

```
Available(wallet) = wallets.balance                              -- folded prefix, up to the watermark
                  + Σ(ledger_entries.amount WHERE id > watermark) -- the un-folded tail
```

The cache may lag the log and the answer is still exact, because the tail sum
covers whatever hasn't been folded yet. A `ledger:materialize` cron (~30s) is the
**sole writer** of `wallets.balance`: it folds the contiguous prefix into `balance`
and advances the watermark **in one atomic CTE** (so balance and watermark always
move together). The per-transaction balance UPDATE is *gone*.

Every balance-affecting operation appends a typed entry (`credit`, `refund`,
`reservation`, `release`, `reversal`, `fee`, `reload`, `withdrawal`, `adjustment`),
so the log also doubles as a perfect audit trail: the entry type plus
`transaction_id` tells you exactly why money moved.

**The central asymmetry — and the whole reason payin and payout scale
differently.** Adding money can never make a balance invalid, so a **credit is a
pure append**: no lock, no read, contends with nothing. Removing money can
overdraw, so a **debit** must guarantee `Available ≥ amount` *and* append the
reservation *atomically* — otherwise two concurrent debits both read "balance =
100," both take 80, and overdraw to −60. That atomic check-then-append needs a
**per-wallet advisory lock**. Therefore:

- **credits → lock-free append → scale to the database's limit;**
- **debits → per-wallet `pg_advisory_xact_lock` + `Available()` guard → serialize
  per wallet** (or batch — see [J7](#j7--the-unified-transaction-write-batcher)).

This asymmetry is why pay-in eventually reached 5,337 while a single hot wallet's
pay-out is bounded by its overdraft lock. It is a *correctness* requirement, not an
inefficiency.

**Proven under load.** 300 concurrent pay-ins produced 300 `credit` entries (exact
sum), but `wallets.n_tup_upd` increased by **5**, not 300 — the fold ticked five
times over 84 seconds; the per-transaction balance UPDATE was eliminated. A
500-payin run showed the balance column updated **once**. The 0-hot-row ratio
improves with *time*, not with TPS. Money-safety was validated exhaustively (append,
fold, checkpoint, advisory-lock, overdraft, release, exactly-once, rebuild,
`Available == Σ`, the 0-hot-row invariant — all green).

One subtle bug caught during reconciliation work: a balance rebuild originally read
`Σ(amount)` and `MAX(id)` in **two separate queries**, and because credits don't
take the wallet lock, a credit committing *between* the two queries could land in
the new watermark but not the balance — a lost entry. Fixed to compute both in one
atomic query. The lesson: **a value-plus-tail derivation must read the cached value
and its watermark in one snapshot**, or a non-locking writer slips between them.

### 4.4 What the schema looks like

The three decisions above are easier to trust with the actual DDL in front of you.
The snippets below are **simplified and representative** — trimmed to the columns
that matter for this article, not verbatim copies of the migrations — but they show
the real shape: daily range-partitioning, UUID v7 defaults, the append-only ledger
with its immutability trigger and folded-cache columns, and the balance derivation.

**1. A daily-partitioned table (`transactions`).** The parent is declared
`PARTITION BY RANGE (created_at)`; the PK is `(id, created_at)` because a partitioned
table's primary key must include the partition key. Each day gets its own child
partition, pre-created by the `partition:create_next` cron.

```sql
-- simplified / representative
CREATE TABLE transactions (
    id          UUID        NOT NULL DEFAULT public.uuidv7(),   -- UUID v7 = time-ordered
    company_id  UUID        NOT NULL,
    wallet_id   UUID        NOT NULL,
    amount      NUMERIC(20,2) NOT NULL,
    currency    CHAR(3)     NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    state       VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',          -- soft delete
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)                                -- must include partition key
) PARTITION BY RANGE (created_at);

-- one child partition per day, created ahead of time by partition:create_next
CREATE TABLE transactions_2026_06_29 PARTITION OF transactions
    FOR VALUES FROM ('2026-06-29 00:00:00+00') TO ('2026-06-30 00:00:00+00');
```

`DEFAULT public.uuidv7()` is what makes the ids chronologically ordered, which is
what lets the `WithUUIDv7PartitionPruning` scope turn a `WHERE id = ?` lookup into a
single-partition scan ([§4.1](#41-the-hotcold-data-model-daily-partitioning)). All 15
high-volume tables follow this exact pattern.

**2. The append-only `ledger_entries` table.** Same daily partitioning, but with two
defining extras: a `BEFORE UPDATE OR DELETE` trigger that makes the row physically
immutable, and a **signed** `amount` so the balance is a plain sum. Note there are
**no foreign keys** — deliberately, for insert speed and immutability
([J2](#j2--fk-for-key-share--multixact-slru-contention)).

```sql
-- simplified / representative  (migration 108)
CREATE TABLE ledger_entries (
    id             UUID          NOT NULL DEFAULT public.uuidv7(),
    wallet_id      UUID          NOT NULL,
    company_id     UUID          NOT NULL,
    transaction_id UUID,
    entry_type     VARCHAR(20)   NOT NULL,    -- credit | reservation | release | refund |
                                              -- reversal | fee | reload | withdrawal | adjustment
    amount         NUMERIC(20,2) NOT NULL,    -- SIGNED: +credit, -debit
    currency       CHAR(3)       NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
    -- no state column, no foreign keys: immutable + fast inserts
) PARTITION BY RANGE (created_at);

-- append-only: physically reject any mutation
CREATE FUNCTION ledger_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only (% rejected)', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_no_update_delete
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();
```

The balance is no longer stored as mutable state on the wallet. Instead, `wallets`
carries a **folded cache** plus a **watermark**:

```sql
-- simplified / representative
ALTER TABLE wallets
    ADD COLUMN balance               NUMERIC(20,2) NOT NULL DEFAULT 0,  -- folded prefix cache
    ADD COLUMN last_batched_entry_id UUID;                              -- watermark
```

**3. The balance formula, as a query.** The exact available balance is the folded
cache plus the sum of every entry newer than the watermark. Because the ids are
UUID v7, `id > watermark` is a clean chronological "everything not yet folded":

```sql
-- simplified / representative
-- Available = wallets.balance (folded prefix) + Σ(ledger tail past the watermark)
SELECT w.balance
     + COALESCE(SUM(le.amount), 0) AS available
FROM   wallets w
LEFT JOIN ledger_entries le
       ON le.wallet_id = w.id
      AND (w.last_batched_entry_id IS NULL OR le.id > w.last_batched_entry_id)
WHERE  w.id = :wallet_id
GROUP  BY w.balance;
```

The `ledger:materialize` cron is the **sole writer** of `wallets.balance`: in one
atomic CTE it folds the contiguous tail into `balance` and advances
`last_batched_entry_id` together, so the cache and watermark never drift apart. The
answer above is exact no matter how far the cache lags.

**4. Dropping the hot-path foreign keys (migration 120).** The FK that
[J2](#j2--fk-for-key-share--multixact-slru-contention) is about — every
`INSERT INTO transactions` taking a `FOR KEY SHARE` lock on the same few parent rows,
spawning MultiXacts that thrash an untunable SLRU on Aurora PG16:

```sql
-- simplified / representative  (migration 120)
ALTER TABLE transactions        DROP CONSTRAINT IF EXISTS transactions_company_id_fkey;
ALTER TABLE transactions        DROP CONSTRAINT IF EXISTS transactions_wallet_id_fkey;
ALTER TABLE provider_statements DROP CONSTRAINT IF EXISTS provider_statements_account_id_fkey;
-- one-time, to clear MultiXacts already accrued on the hot parents:
VACUUM (FREEZE) transactions;
```

Integrity is preserved in the application (parents are validated from cache/DB before
the child is written, and parents are only ever **soft-deleted**), so on these hot,
high-volume, app-validated tables the FK was pure lock cost. FKs are *kept* on the
low-volume tables (users, roles, disputes) where they cost nothing.

### 4.5 The periodic jobs (crontab)

The worker (`cmd/gateway/gateway-worker`) is also the cron scheduler. Every periodic
job is a singleton (only one runs cluster-wide at a time) and its cadence comes from
`app_config`, never hardcoded. The table below is the live set, with the schedule and
the config key that tunes it. Several settle-touching crons are deliberately routed
to the **bounded `settle` queue** ([J8](#j8--the-asynq-redis-self-ddos-and-the-io_multiplier-trap))
so a backlog flush can't flood the single Aurora writer.

| Job | Schedule (default) | Config key | Purpose |
|---|---|---|---|
| `partition:create_next` | daily 02:00 | `partition.advance_days` (30) | pre-create N+30 future daily partitions |
| `partition:detach_old` | daily 02:00 | `partition.detach_after_days` (90) | detach partitions >90 days (all but `audit_logs`) |
| `ledger:materialize` (fold) | every 30 s | `ledger.fold_interval_seconds` (30) | **sole writer** of `wallets.balance` — folds the tail, advances the watermark |
| `ledger:checkpoint` | hourly, minute 7 | `ledger.checkpoint_cron` (`7 * * * *`) | snapshot each folded wallet into `balance_checkpoints` (recovery + rebuild + detach gate) |
| `ledger:detach` (gated) | daily 02:00 | `ledger.detach_gated_enabled` | checkpoint-coverage-gated detach of `ledger_entries` |
| `ledger:seq_verifier` | every 5 min | `ledger.seq_verifier_interval` | read-only continuity check of the entry sequence |
| `provider:balance_flush` | every 30 s | `provider.balance_flush_cron_interval` | drain the Redis system-balance buffer into PG (idle-period fallback) |
| `provider:balance_check` | every 30 s | `provider.check_balance_cron_interval` | refresh provider account balances (idle-period fallback) |
| `transaction:timeout` | every 1 min | `transaction.timeout_check_interval_minutes` | settle timed-out PENDING txns → **`settle` queue** |
| `transaction:pending_status_check` | every 30 min | `transaction.pending_check_cron_interval` | backstop poll for silent (>1h) PENDING chains → **`settle` queue** |
| `reconciliation:payout` | every 5 min | `payout.reconciliation_interval` | reconcile payout state against the provider |
| `recover_orphans` | every 2 min | `scheduler_async_dispatch_recovery_cron` | re-dispatch jobs orphaned by a process exit |
| `webhook:retry` | event-driven | — | retry failed merchant webhook deliveries |
| `payment_link:expiry` | every 5 min | `payment_link.expiry_cron_interval` | expire PENDING links past `expires_at` |
| `idempotency_registry:purge` | daily 03:00 | `idempotency.registry_purge_cron` | delete expired rows from the two idempotency registries |
| `es:sync_retry` | every 5 min | `es.sync_retry_cron_interval_minutes` | retry failed in-process ES buffer writes (skipped under DMS) |
| `es:failure_purge` | daily 03:00 | — | purge stuck in-process ES buffer failures (skipped under DMS) |
| `es:crash_recovery` | every 5 min | `es.crash_recovery_cron_interval_minutes` | recover in-process ES buffer state after a crash (skipped under DMS) |
| `secret:rotate_keys` | every 90 days | `encryption.key_rotation_interval_days` | re-encrypt at-rest secrets onto the active key |

The PostgreSQL→OpenSearch **purge** itself is driven by `es.purge_older_than_days`
(default 90) — the same cutoff the read router uses in
`determineReadSource(date_from, date_to)`: within retention → PostgreSQL, older →
OpenSearch, crossing the cutoff → query both and merge.

---

## 5. The journey

With the foundations in place, the rest was a sequence of walls. Each is reported
in the same shape — **Symptom → Diagnosis (measured) → Decision → Result →
Lesson** — and several begin with a wrong guess that measurement overturned. That
is the point. The headline lesson of the whole campaign is **measure, don't
assume**, and you'll watch it earn its keep repeatedly.

### J1 — The hot wallet (the original wall)

**Symptom.** A single hot wallet capped at **~55 TPS** for debits, no matter how
much hardware we threw at it. Adding CPU or ACUs did nothing.

**Diagnosis.** Three design agents (a ledger specialist, a DB specialist, a skeptic)
traced the per-debit cost to the *physics of the write*: the wallet row lock held
across one fsync plus the statement INSERT. Throughput ≈ 1 / (lock-hold-time
including fsync) ≈ tens of TPS. The balance is a hot row and every money move
serializes on it.

**Decision.** We **rejected sharding the wallet**. Sharding a balance across N
sub-rows breaks the strict global balance chain, introduces a distributed-overdraft
bug (no single place can atomically answer "do we have enough?"), and — decisively —
there are only **1–3** hot keys in the whole system, so there's nothing to spread
across. Sharding solves "many keys, each warm"; our problem is "one key, very hot."
The two real answers were both pursued: make the common move (a credit) lock-free,
and make the unavoidable serialized move (a debit's overdraft check) batched.

**Result.** This step framed the target — and the append-only ledger
([§4.3](#43-the-append-only-ledger)) delivered the lock-free credit. The batched
debit came later in [J7](#j7--the-unified-transaction-write-batcher).

**Lesson.** When one row is the bottleneck, the fix is architectural (change what
you write), not operational (bigger box). And know your access pattern: 1–3 hot keys
means sharding is the wrong tool.

### J2 — FK FOR KEY SHARE → MultiXact SLRU contention

This is the step that took single-wallet pay-in from **85 → 2,200 TPS**, and it's
the most instructive measurement in the whole campaign.

**Symptom.** With the ledger in place, a single hot merchant *still* capped at
**~85 TPS** — and the obvious suspect, Aurora CPU, was only **66%**. The database
was not saturated, yet throughput would not climb.

**Diagnosis (how we measured).** We sampled `pg_stat_activity` directly during a
`-c 400` burst (opening a bastion → Aurora security-group rule; the app stayed on
the proxy so Aurora had free connection slots to answer our `psql`). What we saw:

- **500+ backends parked on `LWLock:BufferContent`**, plus a cluster of
  `MultiXactGen` / `MultiXactOffsetBuffer` / `MultiXactMemberBuffer` /
  `MultiXactOffsetSLRU` waits.
- `pg_stat_statements`: `INSERT INTO transactions` averaging **3,283 ms**,
  `INSERT INTO provider_statements` **866 ms** — slow purely because they were
  *blocked on those locks*, not because the insert is expensive.

The backends were **waiting on locks, not computing** — which is exactly why CPU sat
at 66% and wouldn't rise.

**Root cause — foreign-key `FOR KEY SHARE` locks, explained simply.** When you
INSERT a row with a foreign key to a parent (e.g. `transactions.company_id →
companies.id`), PostgreSQL must guarantee the parent still exists at commit, so it
takes a `FOR KEY SHARE` lock on the parent row — a *shared* lock (many inserts can
hold it; it only blocks someone trying to delete/key-update the parent). Normally
invisible. But our load test used **one** api-key → **one** company → **one** wallet
→ **one** provider account. So *every* concurrent `INSERT INTO transactions` wanted a
`FOR KEY SHARE` lock on the **same few parent rows**. When multiple transactions
share-lock one row, PostgreSQL can't represent that with a single transaction id in
the row header, so it allocates a **MultiXact** (an id standing for "all these txns
share-lock this row"). MultiXacts live in a fixed-size **SLRU cache**. Under a flood
of inserts on the same parents, those caches **thrash** — and **Aurora PostgreSQL 16
does not expose the MultiXact SLRU buffer GUCs** (those tunables only arrived in PG
17). Untunable. The only fix is to stop *creating* the MultiXacts.

**Decision.** The FK guards against a race that **cannot happen here**: parents are
validated from cache/DB before the child is written, and parents are
**soft-deleted** (`state = 'DELETED'`), never hard-deleted. There is no concurrent
DELETE of a company/wallet mid-insert for the FK to protect against. On a hot,
high-volume, app-validated, soft-delete table the FK is pure cost. Migration
`120_20260628_drop_hotpath_fk_multixact_contention.sql` **drops the FK constraints**
on the high-volume partitioned tables that reference mutable hot parents, plus a
one-time `VACUUM (FREEZE)` to clear MultiXacts already accrued. FKs were *kept* on
low-volume tables (users, roles, disputes) where they cost nothing.

**Result.** Same single wallet: **85 → 2,200 TPS** (p99 0.57s). We also confirmed
`synchronous_commit=local` is a **no-op on Aurora** (its durability is the 6-way
storage quorum, not a PG synchronous standby) — flipping it didn't move TPS, which
*confirmed* the FK/MultiXact fix was the real lever, not commit durability.

**Lesson.** Foreign keys are not free under high-concurrency inserts that share
parents: each insert takes a `FOR KEY SHARE` lock, and concentration on a few
parents creates MultiXacts that thrash an untunable SLRU. And note: this was largely
a **test artifact** (one company/wallet) — production traffic across many merchants
spreads the parent locks, which is precisely why all later measurements used
multiple wallets.

### J3 — The misdiagnosed "720 commit wall"

A whole step about being **wrong**, because the discipline is the lesson.

**Symptom.** Earlier in the campaign, after removing three *other* bottlenecks,
throughput plateaued at **~720 TPS**. Raising Aurora's minimum ACU 16 → 48 → 64
moved it only to ~718, and Aurora CPU stayed ~66%.

**What we believed at the time.** "Aurora isn't CPU-bound (more ACU doesn't help),
so the plateau must be the per-payin write work on the single writer — each payin is
two commit transactions (create + settle), commit latency ~9–10 ms — and beating it
needs fewer commits per payin or a multi-writer/sharded DB." We even wrote that into
the README as the conclusion.

**How measurement overturned it.** The instruction was: *keep the proxy, and sample
`pg_stat_activity` directly under load.* That single discipline — look at **wait
events**, don't reason from CPU% — revealed [J2](#j2--fk-for-key-share--multixact-slru-contention):
500+ backends on `LWLock:BufferContent` + MultiXact SLRU. The "720 commit wall" was
never a commit-throughput limit; it was FK/MultiXact contention. Removing the FKs
took the same wallet from 85 to 2,200. The "two commits per payin" story was a
plausible narrative that *fit the CPU number* and was simply wrong.

**Lesson.** **CPU% is a terrible bottleneck oracle for lock-bound work.** Backends
waiting on an LWLock burn no CPU and look exactly like "the DB has headroom but
won't go faster." Always confirm with wait events (`pg_stat_activity`) and
slow-query attribution (`pg_stat_statements`) before naming a bottleneck. We named
this one twice from CPU and were wrong both times.

### J4 — The RDS Proxy saga: starved, not capped

The proxy was accused, convicted, and then exonerated by measurement.

**Act 1.** Goroutines were stuck IO-waiting inside the DB transaction while
`pg_stat_activity` was **empty** — Aurora itself idle. If the backends aren't at
Aurora but the app is blocked, they're queued **at the proxy**, whose `BorrowLatency`
had spiked to **9.6 s**. We concluded "the proxy is the wall" and bypassed it,
pointing the app directly at the Aurora cluster endpoint with bounded per-task pools.
Throughput jumped to ~700 TPS and Aurora was finally exercised. The proxy *looked*
guilty.

**Act 2.** Direct-to-Aurora then hit **connection exhaustion**: 18 tasks × 150
connections = 2,700, and Aurora's `max_connections` was… **844**. The multiplexing
we'd bypassed was the very thing protecting Aurora. So: keep the proxy, and find the
real cause.

**The real root cause.** Back on the proxy, we measured *its* metrics:
`MaxDatabaseConnectionsAllowed` = **226 (!)**, `BorrowLatency` = **29.7 s**, while
`CurrentlySessionPinned` = 9 (so pinning wasn't the problem). 226 backends is
**backend starvation**, not a proxy throughput cap — a proxy can only lease as many
backends as Aurora allows. Why only 226?

**The Aurora Serverless v2 gotcha.** Serverless v2 computes `max_connections` from
`LEAST(DBInstanceClassMemory/9531392, 5000)` **at boot, from the memory at the ACU
it booted at** — and it does **not** rescale as ACU climbs. A cold boot at low ACU
left `max_connections` stuck even after the instance scaled to 256 ACU. Pinning
`max_connections=5000` in the cluster parameter group is necessary **but not
sufficient**: the serverless writer keeps the old value until it **reboots at warm
ACU**.

**Decision.** Pin `max_connections=5000`; **reboot the writer at warm ACU** so the
value actually takes (`SHOW max_connections` → 5000); set the proxy
`MaxConnectionsPercent=100` and `SessionPinningFilters=EXCLUDE_VARIABLE_SETS`.

**Result.** Proxy `BorrowLatency` collapsed **29.7 s → <0.5 s**. No dedicated
PgBouncer was needed — the proxy was never the cap; it was *starved*. We kept it for
its multiplexing, failover, and IAM.

**Lesson.** "The proxy caps at ~1600" was a measurement of a *symptom*. A proxy is
only as good as the backend pool it's allowed to open — check
`MaxDatabaseConnectionsAllowed` before blaming it. And know your managed-DB quirks:
Serverless v2 `max_connections` is computed at boot and doesn't scale with ACU — pin
it *and* reboot at size.

### J5 — "The database is not the bottleneck"

**Symptom.** Even after the FK fix and the proxy fix, we hovered around
~1,700–1,969 TPS, and Aurora CPU at 256 ACU read **94–96%** in the real-callback
flow — which *looked* like "we've maxed the DB; only sharding remains."

**The decisive experiment.** Rather than reason from 96%, we **provisioned a 96-vCPU
`db.r6i.24xlarge` writer** — a 12× bigger box — and re-ran the *same* load.

The 96-vCPU writer ran at **1.3–5% CPU** at the same TPS the 256-ACU serverless
writer had produced at 94–96%. **A 12× bigger writer gave identical throughput.**
Therefore the database has massive headroom; it is *not* CPU-bound. The 94–96% was a
red herring of the smaller instance. The real ceiling was elsewhere — in the shared
singletons (proxy borrow latency, app connection-pool/task-count, single Redis-shard
engine), all clustered in the same band, which is why no single tuning broke past
~2,000.

**Result.** Definitive: the single Aurora writer is not the wall. This reframed the
campaign from "make the DB faster" to "stop serializing the inserts"
([J6](#j6--the-real-single-writer-wall-monotonic-keys--one-wal),
[J7](#j7--the-unified-transaction-write-batcher)) and "scale the singletons."

**Lesson.** When a metric *says* "maxed," **falsify it with an experiment.** A 12×
box that gives the same TPS is irrefutable proof the box was never the limit. This
one run saved us from prematurely building DB sharding.

### J6 — The real single-writer wall: monotonic keys + one WAL

**Symptom.** A single wallet still topped out around **~1,561 TPS**, and spreading
load across **30 wallets** lifted it only to **~2,075** — a mere **+24%**. If the
contention were *per-wallet*, 30 wallets should have multiplied throughput. It barely
moved. Something **global** was serializing the inserts.

**Diagnosis.** `pg_stat_activity` under load, writer CPU only ~20% at 2,000+ TPS
(lock-bound, not CPU-bound). The dominant waits:

- `LWLock:BufferContent` (1,448 backends)
- `LWLock:WALInsert` (659)
- `LWLock:LockManager` (429)

And `INSERT INTO ledger_entries` averaged 46 ms — note `ledger_entries` has **no
FKs**, so this was *not* MultiXact this time. A deeper wall.

**Root cause — monotonic UUID v7 primary keys + a single WAL stream.** UUID v7 is
time-ordered: every new id is larger than the last. Wonderful for partition pruning
and read locality — but on *writes* it means **every insert targets the same
rightmost leaf page of the primary-key B-tree.** To insert there, each backend takes
the buffer-content LWLock on that one page → `LWLock:BufferContent`. Simultaneously,
every insert appends to the **single WAL stream** → `LWLock:WALInsert`. None of this
is per-wallet — the PK hot leaf and the WAL are shared by *all* inserts regardless of
wallet. That's why 30 wallets gave only +24%: the contention was never about the
wallet.

This is a **database-agnostic** hotspot. *Any* monotonically increasing key
(auto-increment, snowflake, UUID v7, timestamp-prefixed) funnels every insert to one
B-tree leaf and one WAL. The classic fix is to *spread* the keys (random/hash), but
that sacrifices the read locality and partition pruning UUID v7 buys us — and we rely
on those heavily. So instead of spreading, we **amortize**: fewer, bigger inserts.

**Lesson.** Monotonic primary keys are an insert hotspot in any database. If
spreading the keys would cost read locality (it would for us), **batch the inserts**
so the per-row lock/flush cost is amortized. And: if N× the keys gives ≪N× the
throughput, your contention is **global**, not per-key — look at the shared
structures (PK leaf, WAL), not the entity.

### J7 — The unified transaction-write batcher

The centerpiece: one mechanism that amortizes LWLock + WAL across the whole
transaction-write path, for both credits (pay-in) and debits (pay-out), at init and
at settle.

**The core idea.** A **single-flight batcher**: concurrent requests hand their work
to a per-shard goroutine, which collects them until `txn.batch.max_size` rows or
`txn.batch.max_wait_ms` elapses, then commits **one** transaction containing **one
multi-row INSERT per table** for the whole batch. N requests → 1 commit → 1 WAL flush
→ 1 hot-leaf lock acquisition. The per-request cost is a small batching wait; the
throughput payoff is large.

It runs in two modes off one generic core:

- **CREDIT mode (pay-in init) — lock-free, cross-wallet.** Credits can't overdraw,
  so the credit batch needs no per-wallet lock and can mix wallets freely. Pure
  amortization of the insert cost.

- **DEBIT mode (pay-out reservation) — one lock, in-memory overdraft pass.** This is
  how we beat the per-wallet serialization of the ledger's debit path:
  1. Acquire the wallet advisory lock **once** for the whole batch → compute
     `Available()` **once** → a `runningAvailable` figure.
  2. **In-memory overdraft pass, no DB round-trips:** accept each reservation while
     `runningAvailable ≥ amount` (decrement on accept), else reject. Because
     Σ(accepted) ≤ Available, **no overdraft is possible** — the exact same guarantee
     as the direct path, computed once in memory under one lock instead of once per
     request under N locks.
  3. **Bulk dedup** via `INSERT … ON CONFLICT DO NOTHING RETURNING transaction_id`
     on the identity registries (merchant reference / external reference /
     idempotency). Keying on `transaction_id`, not the conflict key, correctly
     handles **intra-batch** duplicates (two requests with the same idempotency key
     in the *same* batch).
  4. **Bulk multi-row INSERT** (one statement per table, column-aware chunked under
     PostgreSQL's 65,535 bind-parameter limit) for the accepted set.
  5. Commit **once** (lock released); deliver each per-request result strictly after
     commit.

The **settle path** has the same LWLock wall, so the batcher is hooked into the
centralized `settleOne` funnel — the single entry point for all four settle sources
(PSP callback, the check-status job, the reconciliation/timeout/recover-orphans
crons, and the inline initial-success response). The settle batcher does a **bulk
status-CAS `RETURNING`** (a single `UPDATE … WHERE status = expected RETURNING id`
over the batch, giving exactly-once settlement across processes — only rows whose CAS
won are settled), a **bulk ledger append**, and a **bulk provider-statement** write
whose running balance is computed via an atomic Redis chain (one op per account,
instead of a per-settle row lock on `provider_accounts`).

**Config simplification.** The legacy debit-batcher knobs were consolidated to two —
`txn.batch.max_size` and `txn.batch.max_wait_ms` — **always-on, no enable flag, no
fallback to the direct path**. The no-fallback rule is deliberate and money-critical:
a fallback-to-direct under a control error would flood the very lock the batcher
exists to protect → death spiral. A control error returns **HTTP 503 (retryable)**
instead.

**The adversarial-review discipline (where the safety came from).** Because the
batcher moves money under concurrency, it went through repeated adversarial review
passes framed as money-loss / crash-recovery / data-coherence attacks. They caught
**real** bugs before any deploy:

- a **cross-tenant dedup leak** — a credit shard mixed companies, so a shared `seen`
  set could collide references across tenants → fixed to company-scoped keys;
- a **`WaitGroup` Add/Wait race** in the flush coordination;
- and a **runtime BLOCKER**: a CAS query mixed `$N` and `?` placeholders → mis-bind.
  The unit tests had **mocked** that method, so they passed — it was caught only by a
  **real-SQL test plus a local Docker run against real Postgres**. (For query-builder
  code, test against a real database; a mock can hide a binding bug.)

**Result.**

| Concurrency | Pre-batcher | With batcher |
|---|---:|---:|
| 1,800 | 2,075 | 2,152 |
| 3,000 | collapse | 3,125 |
| 4,500 | collapse | 3,584 (0 err) |
| 6,000 | — | 4,131 |
| 7,500 | — | **4,409 (init peak)** |

At 4,409 TPS the Aurora writer was only **~38% CPU** (vs the contention-bound 2,075).
Money coherence over ~1.1M pay-ins: 0 double-credit, 0 SUCCESS-without-credit, 0
negative balance. The same DEBIT-mode reservation batcher took single-wallet pay-out
**46 → 1,100 TPS** with Aurora CPU only 18% (lock-bound, not DB-bound) and
advisory-lock waiters dropping 418 → 2–4.

**Lesson.** **Batch to amortize LWLock + WAL.** One multi-row INSERT per table per
batch turns the per-row hot-leaf lock and per-row WAL flush into a per-*batch* cost —
that's the whole 2,075 → 4,409 jump, with the writer dropping to 38% CPU. Make the
batcher always-on with no fallback. And for money code, adversarial review + real-DB
tests catch what mocked unit tests structurally cannot.

### J8 — The Asynq Redis self-DDoS and the io_multiplier trap

With the DB unblocked at 38% CPU, the next measured wall was Redis
`EngineCPUUtilization = 99.97%`. The naïve read was "the cache shard is maxed, go
cluster-mode." Measurement said otherwise — and this is the chapter that turned a
*vanity* number into the *honest* one.

**A detour: why Asynq, not Kafka?** Before the diagnosis, it's worth defending the
tool, because "Redis is the bottleneck" invites the obvious question — *shouldn't a
serious payment platform be running Kafka?* The honest answer is no, and the reason
is the same right-sizing instinct that runs through this whole article.

Look at what the async work here actually *is*: **dispatch a side-effecting task and
make sure it happens** — call this PSP, deliver that webhook, index this row to
OpenSearch, run this cron. These are RPC-ish jobs with retries, delays, and
uniqueness constraints. They are **not** a high-throughput event stream, and they are
**not** an event log that several independent consumers replay from their own offsets.
Nobody re-reads the "deliver webhook #5" job six months later from a different
consumer group. The job runs once, succeeds or retries, and is done.

**Asynq** (a Redis-backed Go job queue) is built for exactly that shape, and it gives
us everything the workload needs out of the box: durable retryable jobs,
delayed/scheduled tasks, unique and singleton constraints (so the same provider call
or cron can't double-fire), priority queues, and a dead-letter for what won't
succeed. It runs on the **Redis we already operate** — no new cluster, no new
team-knowledge, minimal ops. The cost of adopting it was nearly zero.

**Kafka** would be over-engineering for this. Kafka is an event-streaming and
distributed-log platform — partitions, consumer groups, offset management, a broker
cluster (plus ZooKeeper/KRaft) to stand up and keep alive. It is *superb* at what
it's for: high-volume event pipelines, stream processing, event sourcing,
multi-consumer fan-out where every consumer replays the same log at its own pace. We
have **none** of those requirements. We don't need a replayable log; we don't need
stream processing; we don't need N independent consumers of the same events. Adopting
Kafka to "make these RPC-ish side-effects happen reliably" would be importing a whole
operational platform to do a job a queue already does — more moving parts, more
failure modes, more to run, for capability we'd never use.

The honest nuance — and it's the very subject of this chapter — is that Asynq's
**single-Redis-node** is itself a throughput ceiling. Asynq's job-queue Lua touches
many un-hash-tagged keys, so it **cannot run on Redis Cluster**; one shard means one
engine core (see the single-threaded-Redis point below), and we hit that wall hard
enough to need its own dedicated node and a recalibrated `io_multiplier`. If this
workload ever *became* a true high-volume event-streaming or multi-consumer fan-out
problem, that's the moment to reconsider — Kafka, or a lighter middle ground like
Redis Streams or NATS. But "reliably dispatch side-effecting tasks" is not that
problem. Choosing the **simplest tool that meets the requirement** — not the most
powerful one available — is the engineering call, and it's the same instinct as
refusing to shard a database the 96-vCPU experiment proved wasn't the bottleneck.

**Diagnosis (measure, don't assume).** First we **split Asynq onto its own dedicated
single-node Redis** (Asynq can't run on Redis Cluster — its job-queue Lua touches
many un-hash-tagged keys → CROSSSLOT). After the split:

- **cache node EngineCPU ≈ 5%** — the cache was *never* the bottleneck.
- **asynq node EngineCPU ≈ 100%**, even with **no active load test running**.

A queue node pegged *while idle* means it's being hammered by the **workers**, not by
request traffic. Two root causes:

1. **`asynq.gateway_worker_io_multiplier = 3000`.** Worker concurrency =
   `NumCPU × io_multiplier`. On 2-vCPU worker tasks that's `2 × 3000 = 6,000`
   handlers *per task*, × 16 tasks = **~96,000 concurrent consumers** all
   polling/dequeuing against a **single-threaded** Redis core. A self-inflicted
   polling storm.
2. **~1.9M transactions stuck in PROCESSING** from earlier init-only tests (their
   settle jobs orphaned). The status-check/timeout cron retried all of them
   continuously → permanent load.

**Why a bigger Redis node wouldn't help.** Redis executes commands on a **single
thread per shard** — that's what makes every command atomic without locks, but it
caps one shard's throughput at one CPU core. A bigger node gives memory and network,
not command throughput. Scaling horizontally (cluster = more shards = more cores)
with a cluster-aware client is the only real fix for the *cache* — and the cache's
ops are almost entirely single-key, so they cluster cleanly. Asynq, which can't
cluster, gets its own isolated node.

**Fix.** `io_multiplier 3000 → 400`, calibrated by **Little's Law**: to sustain
10,000 TPS with a provider call that can take ~1s, you need
`concurrency ≈ throughput × latency = 10,000 × 1s = 10,000` in-flight;
`2 vCPU × 16 tasks × 400 = 12,800` — right-sized with headroom. (25 was far too small
for I/O-bound HTTP to the PSP; 3,000 stormed the single core.) We also cleaned the
~1.87M stuck test transactions (money-safe — no ledger had been appended) and flushed
the asynq node. Result: asynq node **100% → 0.5% idle**.

**The deeper trap.** `io_multiplier` governs **one** worker concurrency pool, but
that pool runs **two opposite workloads**:

- **provider-call jobs** = HTTP to the PSP, **I/O-bound** → want *high* concurrency
  (goroutines mostly waiting on the network).
- **settle jobs** = bulk CAS + ledger append, **DB-bound** → want *bounded*
  concurrency (too many concurrent settle txns flood the single writer).

One knob set high (good for the phone calls) floods the kitchen (the DB writer) with
cooks who trip over each other. The fix was to **split the concurrency** into two
Asynq queues with separate pools — a high pool for the I/O provider-call queue, a
**bounded** pool for the DB-settle queue. With the bounded settle pool deployed, the
settle queue drains to zero and the writer drops from 88% to ~70%, freeing the I/O
pool to run hot without flooding the writer.

**The honesty correction.** The earlier "4,409" was **init-only** — pay-in returns
after *enqueue*, and the settle pipeline was lagging behind a 1.9M backlog. A real
number requires settle to keep up. After calibration (io_multiplier=400, Asynq split,
bounded settle pool, drain between tests), escalating concurrency gave the honest
end-to-end ceiling:

| Path | c=3,000 | c=3,600 | c=4,200 |
|---|---:|---:|---:|
| PAY-IN | 4,227 (0 err) | **5,337 (0 err)** | 5,159 (21 err) |
| PAY-OUT | 4,152 (57 err) | 4,166 (28 err) | 4,074 (120 err) |

**Pay-in sustained e2e ≈ 5,337 TPS, pay-out ≈ 4,150 TPS** — settle keeping up, 0
errors at the sustainable rate. Reads and create-request, which do no settle work,
clear **10,000–11,000 TPS** on the same single writer.

**Lessons.** The 99% you see may not be the 99% that matters — split shared infra to
*attribute* load (cache was 5%, Asynq was the 100%). One concurrency knob for two
workloads is a trap — separate I/O-bound from DB-bound. Calibrate concurrency with
Little's Law, don't guess. And **init-only throughput is a vanity metric** — measure
end-to-end with settle keeping up.

### J9 — The honest ceiling: the test harness itself

Once the settle pool was bounded and the writer freed, the *new* limiter in the test
was the **I/O pool saturating the PSP simulator** — i.e. our test bench's capacity,
not the product's. Per-queue depth measured directly (not `DBSIZE`, which is polluted
by Asynq's retention keys) showed the settle queue draining to zero while the
provider-call queue filled to the full I/O pool depth against the simulator.

In production the PSP is **external**, so that particular ceiling doesn't exist in the
real flow. It's worth recording plainly: the final wall in the test was test
infrastructure, and the product's measured numbers (the table in
[§3](#3-the-results)) are the honest, settle-keeping-up figures below that artifact.

This is the recurring theme made explicit one last time: throughout the campaign,
several confident first guesses — Cloudflare, the proxy-as-cap, the cache-as-wall,
the bigger-writer-will-help — were each disproven by a measurement. Measure, don't
assume.

---

## 6. The configuration that got us there

The knobs and infra choices that, together, produced the numbers above. None of
these alone was the headline lever — the headline lever was always a lock — but each
removed a real blocker and is folded into the final state.

### 6.1 Full configuration reference

The complete set of values that matter, with the value we run and what each one does.
The "tuned" rows are the ones the campaign changed (and migration `122` promotes to
defaults); the rest are the important standing defaults. Every key lives in
`app_config` and is BO-editable; the constants are in
`internal/shared/domain/constants/app_config_keys.go`, seeded by
`internal/shared/infrastructure/persistence/gorm/seeders/app_config_seeder.go`.

*Concurrency & queues (Asynq)*

| Key | Value | Purpose |
|---|---|---|
| `asynq.gateway_worker_io_multiplier` | **400** | worker concurrency = `NumCPU × this` — Little's-Law-calibrated for I/O-bound provider calls (was 3000, which stormed Redis) |
| `asynq.wallet_worker_io_multiplier` | **400** | same, wallet worker |
| `asynq.settle_worker_concurrency` | **64** | bounded DB-settle pool — an **absolute** value (not ×NumCPU), per worker task, on a second Asynq server bound to the `settle` queue |
| `asynq.queue_priority.gateway` | 6 | weighted-random pick under contention |
| `asynq.queue_priority.critical` | 4 | " |
| `asynq.queue_priority.settle` | 6 | " (the dedicated settle server reads only this queue) |
| `asynq.queue_priority.wallet` | 10 | " (wallet worker) |
| `asynq.queue_priority.agent` / `.merchant` | 5 / 5 | reserved weights |
| `asynq.queue_priority.backoffice` | 2 | low-priority background work |

*The transaction-write batcher*

| Key | Value | Purpose |
|---|---|---|
| `txn.batch.max_size` | **500** | rows collapsed into one flush txn = rows in one multi-row INSERT (clamped at 5000) |
| `txn.batch.max_wait_ms` | **10** | max time the batcher waits to accumulate a batch before flushing |

These two are **always-on** with no enable flag and no fallback to the direct path
(a control error returns HTTP 503). They replaced five legacy debit-batcher knobs,
dropped in migration `121`.

*The append-only ledger*

| Key | Value | Purpose |
|---|---|---|
| `ledger.fold_interval_seconds` | **30** | cadence of the `ledger:materialize` fold — the sole writer of `wallets.balance` |
| `ledger.fold_safety_lag_seconds` | **5** | how old an entry's `created_at` must be before it's eligible to fold |
| `ledger.fold_batch_size` | **500** | max wallets folded per run |
| `ledger.checkpoint_cron` | `7 * * * *` | hourly snapshot into `balance_checkpoints` (recovery + rebuild + detach gate) |
| `ledger.detach_gated_enabled` | gate | when off, `ledger_entries` is never auto-detached; when on, only checkpoint-covered partitions detach |

*Partitioning & archival*

| Key | Value | Purpose |
|---|---|---|
| `partition.detach_after_days` | **90** | detach partitions older than this (all but `audit_logs`) |
| `partition.advance_days` | **30** | future daily partitions kept ahead (clamped at 365) |
| `es.purge_older_than_days` | **90** | PG→OpenSearch cutoff — also the read-router boundary |

*Provider system-balance & dispatch*

| Key | Value | Purpose |
|---|---|---|
| `provider.system_balance_redis_chain_enabled` | toggle (default off) | source `system_balance_before/after` from an atomic Redis counter instead of an in-txn `UPDATE … RETURNING` row lock (PG converges via the flush job); falls back to PG on any Redis error |
| `dispatch.global_mode` | `sync` | platform-wide dispatch mode (sync / semi-async / async) |
| `dispatch.enable_custom_per_company` | `true` | when true, mode resolves per-merchant/per-PSP; when false, force the global mode for all txns |

*Runtime (env)*

| Variable | Value | Purpose |
|---|---|---|
| `GOMAXPROCS` | = task vCPU | match Go's P-count to the CFS quota, not the host's CPUs |
| `awslogs` driver | `mode=non-blocking`, `max-buffer-size=25m` | don't block the container on every stdout write |
| `DB_MAX_OPEN_CONNS`, `REDIS_POOL_SIZE`, `ASYNQ_REDIS_POOL_SIZE`, … | env-configurable | connection pools are never hardcoded (`ac435076`) |

*Database (parameter group / managed)*

| Setting | Value | Purpose |
|---|---|---|
| `max_connections` | **5000** | pinned in the cluster parameter group **and** the writer rebooted at warm ACU so it takes (Serverless v2 boot gotcha) |
| hot-path foreign keys | **dropped** | migration `120` + one-time `VACUUM (FREEZE)` — removes `FOR KEY SHARE`/MultiXact contention |
| RDS Proxy | `MaxConnectionsPercent=100`, `SessionPinningFilters=EXCLUDE_VARIABLE_SETS` | keep multiplexing; was starved, not capped |
| `synchronous_commit` | `local` | a no-op on Aurora (durability is the storage quorum) |

The same choices, in prose, with the reasoning behind each:

**Concurrency & queues**
- `asynq.gateway_worker_io_multiplier = 400` (Little's-Law-calibrated for I/O-bound
  provider calls).
- A **bounded settle pool** — a second Asynq server on a dedicated `settle` queue —
  so DB-bound settle work never floods the writer.
- **Asynq on its own single-node Redis**, separate from the cache (Asynq can't run on
  Redis Cluster).
- The **cache on a cluster-capable client** (single-key ops route cleanly across
  shards; multi-key/Lua use hash tags; the SCAN-based invalidation was replaced with
  an explicit key-index `SET`).

**The batcher**
- `txn.batch.max_size` and `txn.batch.max_wait_ms` — always-on, no enable flag, no
  fallback to direct (a control error returns HTTP 503).

**Database**
- `max_connections = 5000` pinned in the cluster parameter group **and the writer
  rebooted at warm ACU** so it actually takes (Serverless v2 boot gotcha).
- FK constraints **dropped** on the hot, high-volume, partitioned tables (migration
  120) + a one-time `VACUUM (FREEZE)`.
- Keep the **RDS Proxy** (`MaxConnectionsPercent=100`,
  `SessionPinningFilters=EXCLUDE_VARIABLE_SETS`) — its multiplexing is essential.
- `synchronous_commit=local` is a no-op on Aurora; don't bother.

**Runtime & logging**
- **`GOMAXPROCS` = the task's vCPU.** Unset, Go sees the host's CPUs while the
  container is CFS-throttled to its task limit → scheduler thrash → multi-second
  latency at "idle" CPU%. Right-size tasks (fewer, beefier beats many tiny — also
  better for the batcher's concentration).
- **Non-blocking `awslogs`** (`mode=non-blocking`, `max-buffer-size=25m`). The
  default driver blocks the container on every stdout write when the CloudWatch pipe
  saturates — under load *every* request, even `GET /health`, stalled ~1.5s with CPU
  and DB idle.
- Connection pools are env-configurable (`DB_MAX_OPEN_CONNS`, `REDIS_POOL_SIZE`,
  `ASYNQ_REDIS_POOL_SIZE`, …), never hardcoded.
- Don't write spam on the hot path — a per-transaction in-app notification INSERT
  (the merchant is already told via the async webhook) added a write + FK lock per
  pay-in; disabled.

**Provider-statement hot row → atomic Redis chain.** The first in-process block found
by `pprof` was `provider_statements.system_balance_before/after` being stamped via an
in-txn `UPDATE provider_accounts … RETURNING`, which held the account row lock for the
whole settle txn → every same-account settle serialized at ~85 TPS. Fix: source
before/after from a globally-atomic Redis running balance (a single Lua
`SET-if-absent seed + INCRBY` on scaled-int minor units = exact, no PG row lock); PG
converges after commit via a flush job. `system_balance` is a reconciliation figure
no payment decision reads, so eventual convergence is fine.

### 6.2 Compute and instance sizing

The reader asked for CPU counts, so here is the concrete fleet — both the standing
ECS Fargate task definitions and the high-water shape used to drive the test.

**Per-task sizes (representative, from the Fargate task definitions).** Services are
sized by profile (`infra/terraform/modules/fargate/locals_services.tf`):

| Service | Profile | Task size | Role |
|---|---|---|---|
| `gateway-api` | `http_hot` | ~**4 vCPU** / tasks | the public pay-in/pay-out/webhook API (CLI-scaled up for the test) |
| `gateway-worker` | `background_worker` | **2 vCPU** | Asynq job consumer + cron scheduler |
| `callback-api` | `http_hot` | **1 vCPU** | inbound provider-callback recorder |
| `psp-simulator` | `simulator` | **0.25–1 vCPU** | the test bench's stand-in for the external PSP (non-prod only) |

(The committed terraform baselines are smaller — `http_hot` is 2 vCPU and
`background_worker` 1 vCPU in prod; the figures above are the scaled-up shapes the
campaign ran. Note: scaling was done by CLI on the running services, because a full
`terraform apply` would reset the fleet to the baseline counts.)

**The fleet at the high-water test:**

| Service | Tasks |
|---|---|
| `gateway-api` | 8 |
| `gateway-worker` | 16 |
| `callback-api` | 12 |
| `psp-simulator` | 24 |

A real constraint here was the **account's Fargate vCPU quota (256)** — at these task
counts and sizes the fleet bumped the quota, which itself capped how far the test
bench could scale.

**The data tier:**

- **Aurora:** PostgreSQL **Serverless v2 at 256 ACU** — the serverless maximum. The
  decisive finding ([J5](#j5--the-database-is-not-the-bottleneck)) was that a
  **provisioned 96-vCPU `db.r6i.24xlarge` writer did not help**: it ran at ~1.3–5%
  CPU producing the same TPS the 256-ACU writer hit at 94–96%. The single-writer wall
  is the WAL + PK-leaf lock ([J6](#j6--the-real-single-writer-wall-monotonic-keys--one-wal)),
  not CPU.
- **Redis (cache):** `cache.r7g.xlarge`, cluster-capable client.
- **Redis (Asynq):** a separate `cache.r7g.large` — Asynq can't run on Redis Cluster,
  so it gets its own single node. (The PSP simulator can take its own node too when
  driving a large test.)

### 6.3 Key file paths

So an engineer can find the code behind each mechanism:

| Concern | Path |
|---|---|
| Unified transaction-write batcher | `internal/gateway/application/usecases/transaction/txn_write_batcher.go` |
| Centralized settle funnel (4 sources) | `internal/gateway/application/usecases/transaction/transaction_settle_one.go` |
| Settle batch flush (bulk CAS + ledger + provider-stmt) | `internal/gateway/application/usecases/transaction/transaction_settle_batch_flush.go` |
| Ledger entry repository | `internal/gateway/infrastructure/persistence/gorm/repositories/ledger/ledger_entry_repository.go` |
| Ledger fold / checkpoint / detach / seq-verifier crons | `internal/gateway/application/jobs/ledger/{materialize,checkpoint,detach,seq_verifier}_job.go` |
| Provider system-balance Redis chain | `internal/gateway/infrastructure/cache/redis/provider_system_balance_chain.go` (contract: `internal/gateway/domain/services/provider_system_balance_chain.go`) |
| Cluster-capable Redis client | `internal/shared/infrastructure/cache/redis/client.go` |
| Asynq server (concurrency + queues) | `internal/shared/infrastructure/jobs/asynq/server.go` |
| Worker + cron registrations | `cmd/gateway/gateway-worker/main.go` |
| Migration tool | `cmd/tools/migrate/main.go` |
| Migrations | `migrations/108_…_wallet_ledger.sql` (ledger), `120_…_drop_hotpath_fk_multixact_contention.sql` (FK drop), `121_…_drop_legacy_wallet_debit_batcher_app_configs.sql` (knob cleanup), `122_…_load_test_tuned_asynq_app_configs.sql` (tuned defaults) |
| In-region load bastion (SSM-driven) | `infra/terraform/modules/fargate` + the flag-gated bastion module (`a88c7902`) |
| Global dispatch-mode override | `dispatch.global_mode` / `dispatch.enable_custom_per_company` (`c38c90b6`) |

---

## 7. Honest limits and what's next

Where the platform genuinely is, and what each remaining ceiling actually requires.

- **Reads and create-request: 10,000–11,000 TPS** on the single writer, today. They
  don't fight the write hotspots, so they're already comfortably past the goal.
- **Pay-in ~5,337 / pay-out ~4,150 TPS** end-to-end on a single Aurora writer, with
  the writer at ~70–88% — money-safe, settle keeping up. The single-writer write
  ceiling for this insert pattern is **~5k**; **10,000 write TPS on one writer is not
  realistic** and we don't pretend otherwise.
- **Throughput vs. latency at the knee — the honest tradeoff.** Those headline write
  numbers are the **maximum capacity**, measured *at* the writer-saturation knee. At
  that point p99 latency stretches to several seconds (~5.5 s pay-in, ~2.0 s pay-out)
  because requests queue in front of the single writer. Below the knee — the
  **comfortable production operating point of ~3,000–4,000 pay-in TPS** — latency is
  low (avg ~0.5–0.6 s, p99 ~1–2 s) with the writer well clear of saturation. This is
  expected single-writer behaviour: as the one writer saturates, throughput peaks and
  the latency tail grows, so the last increment of TPS is bought with tail latency.
  Reads and create-request, by contrast, stay **sub-second (p99 <1 s, 0 errors)** at
  their full 10k–11k TPS because they don't fight the write hotspots. Plan capacity
  against the below-the-knee number, not the saturation peak.

Crossing to **10,000 sustained writes** is a **horizontal** problem, not a config
knob — and the 96-vCPU experiment ([J5](#j5--the-database-is-not-the-bottleneck))
proved the DB has the headroom; the walls are the shared singletons (one writer's
connection fan-in, one proxy, one Redis shard). The plan:

1. **Shard the database by `company_id`** — ~6 Aurora clusters of ~1,700 each. A
   merchant's wallet, transactions, and registries live on one shard, keeping the
   per-wallet overdraft lock and the ledger local. A thin shard-resolver maps the
   shard key to a per-shard connection set.
2. **Redis cluster (~5 shards)** or ElastiCache Serverless for the cache; Asynq stays
   on its own node.
3. **Per-shard PgBouncer (transaction mode) as an ECS service** instead of one shared
   proxy. Transaction-mode pooling is safe here because the per-wallet lock is a
   txn-scoped advisory lock, GORM prepared statements are off, and the money txns are
   tight and DB-only.
4. **Scale the stateless fleet linearly** (gateway-api/worker/callback-api are
   <30% CPU at 1,700 and stateless).

This codebase is unusually well-prepared for that step, which is the point of the
foundations: **UUID v7** keys are already globally unique and time-ordered;
**`company_id` is a natural shard key**; the **append-only ledger** keeps a wallet's
money math local to its shard; the **registries are already hash-partitioned** and
move with their company; and **cross-shard back-office analytics already read from
OpenSearch** (CDC), so sharding the *write* path doesn't break aggregates. Sharding
is well-understood, deferred, and explicitly out of scope here — but the runway is
built.

A note on pay-out: a *single* wallet is bounded by its overdraft lock (~1,100–2,000
TPS with the batcher) **regardless of sharding**, by design — the reservation is an
authorization hold so we never instruct the PSP to disburse funds the merchant
doesn't have under concurrent in-flight pay-outs. Across many wallets/shards, pay-out
scales fine; a single wallet needing >2,000 pay-out TPS is not a realistic case.

---

## 8. Principles

The campaign distilled to a handful of rules. They are database-agnostic;
internalize them and most of this article reproduces itself.

1. **Measure, don't assume.** Every wall here was found by measurement, and several
   first guesses were wrong (the "720 commit wall," the "1600 proxy cap," the
   "Aurora-CPU-bound at 2,200," the "cache is the Redis wall"). Use
   `pg_stat_activity` (wait events), `pg_stat_statements` (slow-query attribution),
   `pprof` (goroutine dumps), and CloudWatch engine metrics. **Falsify "maxed" claims
   with an experiment** — the 96-vCPU writer at 1.3% CPU killed "the DB is the wall"
   in one run.

2. **CPU% is a poor bottleneck oracle for lock-bound work.** Backends waiting on an
   LWLock burn no CPU; the box looks like it has headroom but won't go faster. Confirm
   with wait events before naming the cause.

3. **No hot row.** A single row everyone UPDATEs (balance/counter/sequence)
   serializes the system on a lock held across the commit fsync. Replace it with an
   **append-only ledger** plus a derived, cacheable fold.

4. **Lock-free credits, serialized debits.** Adding money can't break an invariant →
   credits are pure appends, lock-free, scaling to the DB. Removing money can overdraw
   → debits need an atomic per-entity check-then-append → they serialize (or batch).
   This asymmetry is the whole story of pay-in (5,337) vs pay-out (4,150, single
   wallet ~1,100).

5. **Monotonic primary keys are a database-agnostic insert hotspot.** UUID v7 /
   auto-increment / snowflake all funnel every insert to the same rightmost B-tree
   leaf (`LWLock:BufferContent`) and the single WAL stream (`LWLock:WALInsert`). If
   you can't spread the keys (because you need read locality), **batch the inserts**.

6. **Foreign keys cost a shared lock per insert.** Each child INSERT takes
   `FOR KEY SHARE` on its parents; concentration on a few parents creates MultiXacts
   that thrash an untunable SLRU (Aurora PG16). On hot, high-volume, app-validated,
   soft-delete tables, drop the FK and enforce integrity in code.

7. **Batch to amortize LWLock + WAL.** One multi-row INSERT per table per batch turns
   per-row lock + per-row WAL flush into a per-*batch* cost (2,075 → 4,409, writer 38%
   CPU). For an unavoidable lock (overdraft), acquire it once per batch and do the
   overdraft pass in memory. Never fall back from the batcher to the direct path —
   fallback floods the lock you're protecting; return a retryable 503.

8. **Redis is single-threaded per shard.** One engine core caps one shard's command
   throughput; a bigger node gives memory/network, not throughput. Scale
   **horizontally** (cluster = more cores), make the client cluster-aware, and isolate
   workloads that can't cluster (Asynq) onto their own node.

9. **One concurrency knob for two workloads is a trap.** Separate I/O-bound
   (provider HTTP, wants high concurrency) from DB-bound (settle, wants bounded
   concurrency). Calibrate each with Little's Law (`throughput × latency`), don't
   guess.

10. **Init-only throughput is a vanity metric.** Measure end-to-end with the settle
    pipeline keeping up; a number that depends on a growing backlog is not a number.

11. **Know your managed-service quirks.** Aurora Serverless v2 `max_connections` is
    computed at boot from ACU and doesn't rescale — pin it *and* reboot at size, or
    your proxy starves. `synchronous_commit` is a no-op on Aurora. A managed proxy can
    only lease as many backends as the DB allows.

12. **Compute derived state in one snapshot.** A balance = "cached value + tail sum"
    must read the value and its watermark in one atomic query, or a non-locking credit
    slips between two queries and is lost. (And any document's index must be a pure
    function of its `created_at`, never wall-clock-now — the same idea, in the search
    tier.)

13. **For money code, adversarial review + real-DB tests are non-negotiable.** Mocked
    unit tests passed while a `$N`/`?` placeholder mis-bind sat in a CAS query; only a
    real-SQL test plus Docker Postgres caught it. Adversarial passes caught a
    cross-tenant dedup leak and a WaitGroup race before any deploy. And don't be
    precious about code — the first hot-wallet batcher was deleted when the ledger made
    it redundant. Right idea, wrong layer; removing it was progress.

---

*Built for West Africa, measured on AWS eu-west-3, and written so the next engineer
inherits the reasoning, not just the final state.*

**— Moussa Ndour, Infrastructure Engineer**
macbookpro@192 nexusypay % 
