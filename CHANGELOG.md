# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The stable binary ABI promise (`Compatibility.BinaryCompatible` under `sbt-version-policy`)
applies to the following published modules on the 1.x line:

- `tasks-shared`
- `tasks-core`
- `tasks-circe`
- `tasks-upickle`
- `tasks-spores`

Backend modules (`tasks-batch`, `tasks-ecs`, `tasks-s3`, `tasks-postgres`,
`tasks-dynamo`, `tasks-kubernetes`) ship under `Compatibility.None` and may
change their public API across minor releases.

## [Unreleased]

### Added

- `tasks.shared.VersionedResourceRequest.unapply` and
  `tasks.shared.VersionedResourceAllocated.unapply` on their new sealed-trait
  companions, so existing pattern-matching code (e.g.
  `tasks.elastic.SimpleDecideNewNode`) keeps compiling.
- `tasks.shared.ResourceAllocated.unapply` on its new sealed-trait companion.
- `CHANGELOG.md` at the repo root.

### Changed

- `tasks.shared.ResourceRequest`, `ResourceAllocated`, `ResourceAvailable`,
  `Replication`, `VersionedResourceRequest`, `VersionedResourceAllocated`,
  `VersionedResourceAvailable`: each converted from `case class` to
  `sealed trait` + `private[tasks]` case class implementation. All existing
  companion `apply` overloads preserved; the on-wire jsoniter bytes are
  identical to the previous case-class format via a hand-written delegating
  codec. Pattern-matching call sites that destructured these types continue
  to work via companion `unapply`. Users calling `.copy()` on these types must
  switch to constructing a fresh instance via `apply(...)` — the five
  internal `.copy()` call sites in `TaskSystemComponents`, `ProcessContext`
  (wither methods), `BatchElasticSupport`, `NodeRegistryStateTest`,
  `NodeSelectorTest`, `EcsPlacementChainTest`, and
  `EcsNodeSelectorPlacementTest` have been updated.
- `tasks.elastic.process.DockerConfig`: converted from `final case class` to
  `sealed trait` + `private[tasks]` case class implementation. All existing
  wither methods (`withImage`, `withNetwork`, `withEnvironment`, `withContext`)
  reconstruct via the public `apply`.
- `tasks.fileservice.SharedFile`: converted from `case class` to `sealed trait`
  carrying all methods (`file`, `filePersistent`, `fileNonCached`, `history`,
  `stream`, `isAccessible`, `uri`, `name`, `delete`, `utf8`, `bytes`) + a
  `private[tasks]` case class `SharedFileImpl` holding the three content-
  addressed fields (`path`, `byteSize`, `hash`). Companion `apply(path, byteSize,
  hash)` constructs, `unapply` restores pattern-matching, hand-written
  delegating `JsonValueCodec` preserves wire bytes. `tasks.upickle`'s
  `sharedFileRW` bijects through the private impl to keep upickle serialization
  working.
- Full sealed-trait + `private[tasks]` case class impl + hand-written delegating
  codec treatment applied to every remaining public case class in the stable
  track: `tasks.queue.TaskId`, `tasks.queue.HashedTaskDescription`,
  `tasks.shared.CodeVersion`, `tasks.shared.Priority`,
  `tasks.shared.RunningJobId`, `tasks.shared.PendingJobId`,
  `tasks.shared.Labels`, `tasks.shared.LogRecord`,
  `tasks.shared.NodeSelector.Has`, `.Not`, `.And`, `.Or` (the parent sealed
  hierarchy keeps the five discriminator tags via a `private[shared]` `view`
  mirror hierarchy and a bijection codec), `tasks.util.Uri`,
  `tasks.util.SimpleSocketAddress`, `tasks.fileservice.ManagedFilePath`,
  `tasks.fileservice.RemoteFilePath`, `tasks.queue.Spore[A,B]`,
  `tasks.queue.SerDe[A]`. Every reshaped type carries `unapply` where pattern
  matching required it. `tasks.upickle` `uri`, `rpath`, `mpath` adjusted to
  biject through the respective private impls.
- Types that nothing outside the `tasks.*` package needs to see are kept as
  plain `private[tasks] case class` instead of being wrapped in the sealed-trait
  pattern (the ABI-lock concern does not apply to private symbols):
  `tasks.queue.Base64Data`, `tasks.shared.ElapsedTimeNanoSeconds`,
  `tasks.util.CorrelationId`, `tasks.queue.SporeException`,
  `tasks.queue.Spore.SporeDTO`, `tasks.queue.UntypedResult`,
  `tasks.queue.UntypedResultWithMetadata`, `tasks.queue.Proxy`.
  `tasks.util.concurrent.NodeLocalCache.State[A]` was already in this category;
  its enclosing `object` is now also `private[tasks]` so the whole plumbing
  object is sealed off (user-facing API is `tasks.queue.NodeLocalCache` whose
  methods take `TaskSystemComponents` and return `Resource[IO, A]`).
- `tasks.ExternalQueueState` was removed. The wrapper's job was to keep
  `tasks.queue.QueueImpl.State` out of `withTaskSystem`'s public signature,
  but Scala accepts a public method whose parameter type is `private[tasks]`
  — external callers can hold such a value and pass it along without naming
  the type. The signatures of `withTaskSystem` and `defaultTaskSystem` now
  take `Resource[IO, Option[Transaction[tasks.queue.QueueImpl.State]]]`
  directly. The backend `makeTransaction` factories in `tasks-postgres`,
  `tasks-dynamo`, `tasks-s3` return `Resource[IO, Transaction[QueueImpl.State]]`
  straight through; external users receive the value and plug it into
  `withTaskSystem` with no wrapping or unwrapping anywhere. `Transaction.imap`
  and `ExternalQueueState.scala` are gone.
- `tasks.elastic.ElasticSupport`: converted from `final class` to a public
  `trait` with concrete-default methods for `convertRunningToPending` and
  `needsPackageServer`, plus a `private[tasks]` case-class implementation and
  three `apply` overloads on the companion. All `new ElasticSupport(...)` call
  sites in core, backends (`batch`, `ecs`, `kubernetes`), and tests
  (`CompositeElasticSupport`, `ProcessElasticSupport`, `CompositeElasticSupportTest`,
  `JvmElasticSupport`, `AppOnlyMasterTest`, `MaxNodesCumulativeRaceTest`,
  `RequestNodeBatchSizeTest`) updated to use the companion factory.
- `tasks.util.config.TasksConfig`: 61 members demoted to `private[tasks]`.
  Members that stay public: `raw`, `hostLabels`, `hostName`, `hostNumCPU`,
  `hostRAM`, `hostScratch`, `nodeName`, `workerHealthUrlFile`.
  Trait `ConfigValuesForHostConfiguration` also had its unused host members
  (`hostImage`, `hostGPU`, `hostNameExternal`, `hostPort`, `mayUseArbitraryPort`,
  `masterAddress`, `masterPrefix`, `startApp`) demoted.
- `tasks.util.concurrent.NodeLocalCache.State[A]`: narrowed to `private[tasks]`.
- Every nested type under `tasks.queue.QueueImpl.*` narrowed to `private[tasks]`:
  `State`, `Event` + its 17 children, `LauncherStopReason` + its 3 children,
  `ProxyResult` + its 2 children, `Dispatch`, `ScheduledTask`,
  `ScheduleTaskEqualityProjection`, and the `project` helper. Now that
  `ExternalQueueState` sits in front of the public API, nothing outside the
  `tasks.*` package can import these symbols. The companion object itself
  stays public (as required by its `private[tasks] class QueueImpl` sibling)
  but holds no publicly-nameable members.
- `scalacOptions`: Scala 3 flag `-experimental` removed from `tasks-shared`,
  `tasks-circe`, `tasks-upickle`. Retained on `tasks-core` and `tasks-spores`
  because their macros use `@scala.annotation.experimental`.
- `build.sbt`: `versionPolicyIntention := Compatibility.BinaryCompatible` set
  on each of the five stable modules via `stableBinarySettings` /
  `stableBinarySettingsKeepExperimental`. The declaration is active now; the
  gate becomes meaningful once the next release is published as the new
  baseline for `versionPolicyCheck`.

### Removed

- Vestigial `tasks.shared.monitor.QueueStat` case class plus its unused
  `@SerialVersionUID(1L)` annotation. No Java serialization anywhere in the
  tree used it; the live type remains `tasks.util.message.QueueStat`.
- Dead `import tasks.shared.monitor._` from
  `tasks.elastic.NodeRegistryState`.
- Dead jsoniter codecs that no durable path reaches:
  - `tasks.queue.DependenciesAndRuntimeMetadata.codec`
  - `tasks.queue.SerDe.codec`
  - `tasks.jsonitersupport.sharedFileCodec` (the equivalent codec on
    `tasks.fileservice.SharedFile`'s companion remains in scope)
