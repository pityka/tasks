---
name: using-tasks
description: >
  How to use the pityka `tasks` Scala library (io.github.pityka/tasks-*) as a
  client: define memoized `Task`/`ParentTask`, stand up a task system with
  `withTaskSystem`, submit with a `ResourceRequest`, and — above all — reason
  correctly about persistent caching and cache replay (what the cache key is,
  how to invalidate it, `SharedFile` outputs, and replaying a cached workflow
  with no workers and no queue-state writes). Use when writing or reviewing
  Scala that imports `tasks._`, defines `Task(...)`/`ParentTask(...)`, calls
  `withTaskSystem`/`defaultTaskSystem`, or depends on tasks-core / tasks-s3 /
  tasks-postgres.
---

# Using the `tasks` library

`tasks` gives you **persistent memoization of asynchronous, distributed
computations**. A `Task[A, B]` is a named, versioned function `A => IO[B]` whose
result is cached on durable storage. Submit a task with a given input twice and
the body runs once; the second submission returns the stored result without
re-executing. The cache survives process restarts and is shared across every
worker in a deployment, so memoization works across machines and across runs.

Everything a client needs to get right clusters around one idea: **task
identity**. Get identity right and caching, invalidation, and replay all follow.

---

## 1. The 30-second mental model

```
submit task(input)
      │
      ▼
compute cache key = (task name, task version, hash(serialized input))
      │
      ├── key present in storage ──► return stored result. No worker. No compute.  ← a cache hit
      │
      └── key absent ─────────────► schedule to a worker ─► run body ─► upload output
                                     files as SharedFiles ─► write cache entry ─► return
```

- The cache key is **task name + task version + a SHA-256 of the serialized
  input bytes**. Nothing else.
- The **resource request** (CPU/memory/GPU) and the build-derived **code
  version** are *not* part of the key. Two submissions that differ only in how
  much CPU they ask for hit the same entry.
- **Cache replay** is not a separate API. It is simply what happens when you run
  your ordinary submission code against a storage location that already holds the
  results: every submission resolves from the cache.

---

## 2. Setup and a complete example

Dependencies (`build.sbt`): add `tasks-core`; add `tasks-s3` for S3 storage,
`tasks-postgres` for a durable shared queue.

Two imports carry the API and the serialization bridge:

```scala
import tasks._
import tasks.jsonitersupport._
```

Task input and output types each need a jsoniter `JsonValueCodec` in implicit
scope. `tasks.jsonitersupport._` bridges any `JsonValueCodec` to the library's
internal `Serializer`/`Deserializer` and supplies codecs for the standard types.

```scala
import tasks._
import tasks.jsonitersupport._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import cats.effect.IO

object Jobs {

  case class In(n: Int)
  object In {
    implicit val codec: JsonValueCodec[In] = JsonCodecMaker.make
  }

  case class Out(n: Int)
  object Out {
    implicit val codec: JsonValueCodec[Out] = JsonCodecMaker.make
  }

  val increment: TaskDefinition[In, Out] =
    Task[In, Out]("increment", 1) { in => implicit env =>
      IO.pure(Out(in.n + 1))
    }
}
```

Task definitions are checked at compile time:

- A task must be assigned to a **stable identifier** — a `val` on an `object`.
- Its body may **not close over lexical scope** except stable values (`val`
  members of objects). Put everything the body depends on into the input.
- The input type must be `<: AnyRef` and both input and output must have codecs
  in implicit scope.

Stand up a task system and submit. Submission is an ordinary function call that
returns `IO[B]`:

```scala
import org.ekrich.config.ConfigFactory

val program: IO[Either[cats.effect.ExitCode, Out]] =
  withTaskSystem(
    ConfigFactory.parseString("tasks.fileservice.storageURI=/data/tasks")
  ) { implicit ts =>
    Jobs.increment(In(41))(ResourceRequest(cpu = 1, memory = 1000))
  }
```

- `tasks.fileservice.storageURI` is an absolute path (local FS or an
  NFS-style shared mount) or an `s3://bucket/prefix` URI. **This is where the
  cache lives.** It defaults to `./`.
- `withTaskSystem(...)( use )` returns `IO[Either[ExitCode, T]]`. On an
  application process the `use` block runs and you get `Right(result)`. On a
  pure worker process the block does not run; the `IO` stays alive pulling jobs
  and yields `Left(exitCode)`.
- `ResourceRequest` has several overloads: `ResourceRequest(cpu, memory)`,
  `ResourceRequest(cpu, memory, scratch)`, `ResourceRequest((min, max), memory,
  scratch, gpu)`, plus variants taking a `NodeSelector` or `Replication`.
  Memory and scratch are in MB.

For S3 storage, pass a client:

```scala
withTaskSystem(
  config = "tasks.fileservice.storageURI=s3://my-bucket/prefix",
  s3Client = tasks.fileservice.s3.S3.makeS3ClientResource("us-east-1"),
  elasticSupport = cats.effect.kernel.Resource.pure(None)
) { implicit ts => ??? }
```

---

## 3. What the cache key is (and is not)

The key is a `HashedTaskDescription(TaskId(name, version), dataHash)`:

| Part | Source | Notes |
|---|---|---|
| **name** | first arg of `Task[A,B]("name", v)` | your stable identity for the task |
| **version** | second arg (`Int`) | your explicit invalidation knob |
| **dataHash** | SHA-256 of the serialized input bytes | via the input's codec |

The stored entry is a small metadata file (`__meta__result__<name>-<version>-<hash>`)
holding the serialized output plus references to any `SharedFile`s the result
points at. By default it sits under the task's file prefix in your storage; set
`tasks.cache.sharefilecache.path` to a relative folder to collect all cache
metadata in one place instead.

**Not in the key:** the resource request, the number of CPUs, the code version,
labels, priority, or anything about the worker. If you submit the same task with
the same input but a bigger `ResourceRequest`, you still get the cached result.
(The queue logs a `ResourceRequestDiverges` warning if the *same* task is queued
concurrently with two different resource requests, because only one can win.)

---

## 4. Cache replay — the part to get right

"Cache replay" is re-running your submission code when the results already exist
in storage. This happens constantly: a re-run of the app after a crash, a
downstream job that re-submits the same upstream tasks, a second analysis over
the same inputs. You do nothing special — you just submit the same tasks against
the same `storageURI`. The properties that make this cheap and safe:

**1. No workers, no nodes, no compute.** On a cache hit the task is never
enqueued and never scheduled. No worker is allocated and — with an elastic
backend — no node is requested. You can replay an enormous DAG on a process
configured with almost no local CPU and no scaling backend at all.

**2. Cache hits stay out of the shared queue state.** A hit is delivered through
a **process-local** in-memory map, not the queue's durable `completedResults`.
This matters for the Postgres-backed HA queue: replaying a large cached workflow
performs **zero** completed-result writes against the shared transactional
state. The regression test `CacheHitExternalQueueNoWriteTest` submits a task
once to populate, then replays it 20 times against an external queue state and
asserts the write count is exactly `0`. Replay is read-only pressure on the
queue backend.

**3. Replay latency is tunable independently of write traffic.** A waiting
caller learns its result by polling the queue at `tasks.resultPollInterval`
(default `100 ms`) — a read-only poll, deliberately far more frequent than the
write-heavy worker/queue ping `tasks.askInterval` (`500 ms`). Lower
`resultPollInterval` to shorten the delivery latency of a big cache-hit replay
without adding any write load.

**4. A cached parent short-circuits its entire subtree.** `ParentTask` results
are cached like any other, keyed by the parent's input. On replay a cached
parent returns its stored result **without running its body**, so its children
are never submitted. Cache the top of a computation and everything beneath it is
skipped. (This is why parent bodies must be pure functions of their input, same
as leaves.)

**5. Replay is verified against storage.** With `tasks.verifySharedFileInCache =
true` (the default) a hit is honored only if every `SharedFile` referenced by
the stored result is still accessible. If someone deleted an output file from
storage, that entry is treated as a miss and the task recomputes. Set it to
`false` to trust the cache blindly and skip the accessibility check (faster, but
a dangling reference then surfaces later when you read the file).

**6. Replay degrades gracefully on an unreadable entry.** If a stored result
cannot be deserialized into the current output type, the caller logs the failure
and reschedules the task **without caching**, i.e. it recomputes rather than
hard-failing. See the gotcha about output-type changes below.

---

## 5. Invalidation and the gotchas that bite

**To invalidate deliberately, bump the version integer** in the task definition.
That is the one intended knob:

```scala
val increment = Task[In, Out]("increment", 2) { in => implicit env => ... }
```

- **Changing the body without bumping the version serves a stale result.** The
  cache never inspects the body — only name, version, and input hash. If you fix
  a bug in a task body and want old results discarded, you *must* increment the
  version. This is the single most common mistake.
- **Changing the output type is subtle.** Different input *shape* changes the
  serialized bytes and so naturally produces a new key. But a changed *output*
  type does not change the key. Old bytes may still deserialize into the new type
  (giving a quietly wrong value) or may fail to deserialize (triggering the
  recompute-without-cache path in §4.6). Either way: bump the version when the
  output meaning changes.
- **Non-deterministic or side-effecting tasks should not be memoized as pure.**
  If a task's output legitimately varies for the same input (a timestamp, a
  random draw, a call to a mutating external system), either fold the varying
  part into the input so it becomes part of the key, or pass `noCache = true`.
- **`noCache = true` suppresses the write, not the read.** It keeps *this*
  submission's produced result out of the cache. It does **not** skip the
  lookup: if an entry already exists it is still returned. To force a fresh
  computation you need a key that misses (bump the version, or vary the input) —
  `noCache` alone will happily replay a stale entry.

  ```scala
  Jobs.increment(In(41))(ResourceRequest(cpu = 1, memory = 1000), noCache = true)
  ```

- **Disable caching entirely** with `tasks.cache.enabled = false`: every lookup
  misses and nothing is ever written, so every submission executes.

---

## 6. Files as cached values (`SharedFile`)

Don't put large bytes in a task result — return `SharedFile` handles. A
`SharedFile` is an opaque, content-addressed reference: the bytes live in the
file service, and only the small handle is serialized into the cache entry. On a
cache hit you get the handle back immediately and fetch bytes lazily, on any
machine, only if you read them. This is what keeps replay of file-producing
workflows cheap.

Inside a task body, create files with the **scoped** constructors — their names
are prefixed by the task's input-derived path, so they can't silently collide
across invocations:

```scala
val write = Task[In, SharedFile]("write", 1) { in => implicit env =>
  SharedFile.scoped(
    fs2.Stream.chunk(fs2.Chunk.array(in.n.toString.getBytes("UTF-8"))),
    suffix = "value.txt"
  )
}
```

Read them back with `.stream` (fs2 byte stream), `.file` (a local temp file in a
`Resource`), `.utf8`, or `.bytes`.

Creating a `SharedFile` from data requires a task: the `scoped` constructors
take the `ComputationEnvironment`, so they only compile inside a task body.
Outside a task there is no constructor that writes new bytes into the store —
bring external data in by **linking** it: `SharedFile(uri)` references an
existing `file://`, `s3://`, or `https://` resource without copying it.

To reference files a task takes as input, hold them as `SharedFile` members of
the input case class; the library also verifies and traces those.

---

## 7. Parent tasks (spawning subtasks)

A `ParentTask` submits children from its body. It runs with a zero resource
request and its body receives a `ParentComputationEnvironment` (the capability
that lets it submit children — leaf task bodies cannot submit tasks):

```scala
val fib: ParentTaskDefinition[In, Int] =
  ParentTask[In, Int]("fib", 1) { in => implicit cxt =>
    in.n match {
      case 0 => IO.pure(0)
      case 1 => IO.pure(1)
      case n =>
        for {
          a <- fib(In(n - 1))
          b <- fib(In(n - 2))
          r <- Jobs.reduce(Reduce(a, b))(ResourceRequest(cpu = 1, memory = 1))
        } yield r
    }
  }
```

Submit a parent with `fib(In(10))` (no resource request; it takes optional
`priorityBase`, `labels`, `noCache`). Because parent results are cached, a
re-run resolves `fib(In(10))` straight from cache and never re-expands the
recursion — see §4.4.

---

## 8. Configuration reference (caching-relevant keys)

Config is HOCON via sconfig; pass a `Config` (or a string) to the constructor,
or override any key with a system property. Full defaults live in
`core/src/main/resources/reference.conf`.

| Key | Default | Effect |
|---|---|---|
| `tasks.fileservice.storageURI` | `./` | Where results and the cache live; absolute path or `s3://…`. |
| `tasks.cache.enabled` | `true` | `false` turns off all read and write; everything recomputes. |
| `tasks.verifySharedFileInCache` | `true` | Honor a hit only if its files are still accessible (see §4.5). |
| `tasks.cache.sharefilecache.path` | `prefix` | `prefix` = store metadata beside outputs; a relative path = collect it in one folder. |
| `tasks.cache.timeout` | `10 minutes` | How long a worker waits to persist a result before giving up. |
| `tasks.cache.accessibility-check-parallelism` | `32` | Concurrency of the file-accessibility check on a hit. |
| `tasks.resultPollInterval` | `100 ms` | Caller's read-only poll for its result; lower it to speed up replay delivery (§4.3). |
| `tasks.askInterval` | `500 ms` | Worker/queue write-heavy ping; unrelated to replay latency. |

For horizontal scaling of the application role (multiple identical app
processes), use `tasks-postgres` to externalize the queue state **and** keep the
cache on globally accessible storage (S3, or an NFS mount with atomic rename) —
or disable persistent caching. The no-write-on-replay property in §4.2 is what
keeps that shared Postgres state from becoming a replay bottleneck.

---

## 9. Client checklist

- [ ] `import tasks._` and `import tasks.jsonitersupport._`.
- [ ] Every task input/output has an implicit `JsonValueCodec` (in its companion).
- [ ] Each task is a `val` on an `object`; body closes over nothing but stable vals.
- [ ] Set `tasks.fileservice.storageURI` to shared, durable storage.
- [ ] Task **name is stable**; **bump the version** whenever the body or output meaning changes.
- [ ] Anything that should distinguish two results is in the **input** (not the resource request, not the code version).
- [ ] Large outputs are returned as `SharedFile.scoped(...)`, not inline bytes.
- [ ] Non-deterministic tasks either fold the variance into the input or pass `noCache = true`.
- [ ] For replay of a big cached DAG: keep storage reachable, rely on cache hits needing no workers, and lower `tasks.resultPollInterval` if delivery latency matters.
