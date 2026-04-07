# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scala library for persistent memoization of asynchronous distributed computations, with support for remote execution on AWS EC2, Kubernetes, and SSH. Published to Maven Central as `io.github.pityka/tasks-*`.

## Build Commands

```bash
sbt compile                  # Compile all modules
sbt +testables/test          # Run all tests (cross-compiled: Scala 2.13 + 3)
sbt core/test                # Run core module tests only
sbt "core/testOnly tasks.ParallelSubmissionTest"  # Run a single test class
sbt scalafmtAll              # Format all source files
sbt +testables/test versionPolicyCheck  # Full CI check
```

- SBT 1.12.0, Java 17, Scala 2.13.18 / 3.6.4
- Formatting: scalafmt 3.8.3 (dialect: scala213, with scala3 override for `scala-3/` directories)

## Architecture

**Task lifecycle:** Define task → Submit with ResourceRequest → Queue schedules to worker → Execute → Cache result persistently → Return cached on re-submission with same input.

**Key modules:**
- `core/` — Queue (in-memory), file service (local FS), caching, elastic scaling, messaging, task execution
- `shared/` — Cross-module types: `ResourceRequest`, `ResourceAllocated`, `ResourceAvailable`, `HashedTaskDescription`
- `postgres/` — Durable PostgreSQL-backed queue for HA deployments
- `s3/` — S3 file storage backend
- `ec2/`, `kubernetes/`, `ssh/` — Elastic scaling backends
- `spores/` — Closure serialization (Scala 2.13 only, compile-time purity verification)
- `circe/`, `upickle/` — Alternative JSON serialization support (core uses jsoniter-scala)

**Core internals (`core/src/main/scala/tasks/`):**
- `package.scala` — Public API: `Task[A,B]`, `ResourceRequest`, `defaultTaskSystem`, `withTaskSystem`
- `TaskSystemComponents.scala` — Central coordinator holding queue, file service, cache, messenger
- `queue/QueueImpl.scala` — In-memory queue with resource-aware scheduling
- `queue/Launcher.scala` — Worker node lifecycle management
- `fileservice/SharedFile.scala` — Opaque file references with content-addressed storage
- `caching/TaskResultCache.scala` — Persistent memoization keyed by task name + version + serialized input hash
- `elastic/` — Pluggable backends for auto-scaling workers based on queue demand

**Configuration:** HOCON via sconfig. Defaults in `core/src/main/resources/reference.conf`. Override via system properties or `ConfigFactory.parseString` passed to constructors.

## Testing

Tests are in `core/src/test/scala/tasks/`. Test trait `TestHelpers` (in `testhelpers.scala`) provides:
- `testConfig` — temp directory storage, 4 CPUs
- `await` — 60-second timeout for IO/Future

Tests use ScalaTest `AnyFunSuite`. Most tests stand up a full `TaskSystem` via `withTaskSystem(config)` and exercise end-to-end task submission, caching, and resource allocation.

## Serialization

Task inputs/outputs require implicit `JsonValueCodec[T]` (jsoniter-scala). Derive with `JsonCodecMaker.make`. Circe and uPickle codecs also supported via their respective modules.
