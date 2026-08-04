package tasks.queue

import tasks.queue.QueueImpl._
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.LauncherName
import tasks.util.message.RendezvousGroupId
import tasks.util.message.Node
import tasks.shared.VersionedResourceAllocated
import tasks.shared.PendingJobId
import tasks.shared.RunningJobId
import tasks.elastic.NodeRegistryState

private[tasks] case class SerializableQueueState(
    queuedTasks: List[
      (ScheduleTaskEqualityProjection, (ScheduleTask, List[Proxy]))
    ],
    scheduledTasks: List[
      (
          ScheduleTaskEqualityProjection,
          (
              LauncherName,
              VersionedResourceAllocated,
              List[Proxy],
              ScheduleTask
          )
      )
    ],
    knownLaunchers: List[(LauncherName, Option[Node])],
    counters: List[(LauncherName, Long)],
    nodes: NodeRegistryState.State,
    rendezvous: List[(RendezvousGroupId, QueueImpl.RendezvousGroup)] = Nil
) {
  def toState = QueueImpl.State(
    queuedTasks = queuedTasks.toMap,
    scheduledTasks = scheduledTasks.toMap,
    knownLaunchers = knownLaunchers.toMap,
    counters = counters.toMap,
    nodes = nodes,
    rendezvous = rendezvous.toMap
  )
}

private[tasks] object SerializableQueueState {
  def fromState(state: QueueImpl.State) = SerializableQueueState(
    queuedTasks = state.queuedTasks.toList,
    scheduledTasks = state.scheduledTasks.toList,
    knownLaunchers = state.knownLaunchers.toList,
    counters = state.counters.toList,
    nodes = state.nodes,
    rendezvous = state.rendezvous.toList
  )

  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._

  implicit val pendingJobIdKeyCodec: JsonKeyCodec[PendingJobId] =
    new JsonKeyCodec[PendingJobId] {
      def decodeKey(in: JsonReader): PendingJobId = PendingJobId(
        in.readKeyAsString()
      )
      def encodeKey(x: PendingJobId, out: JsonWriter): Unit =
        out.writeKey(x.value)
    }

  implicit val runningJobIdKeyCodec: JsonKeyCodec[RunningJobId] =
    new JsonKeyCodec[RunningJobId] {
      def decodeKey(in: JsonReader): RunningJobId = RunningJobId(
        in.readKeyAsString()
      )
      def encodeKey(x: RunningJobId, out: JsonWriter): Unit =
        out.writeKey(x.value)
    }

  implicit val codec: JsonValueCodec[SerializableQueueState] =
    JsonCodecMaker.make

  private val readerConfig = ReaderConfig
    .withMaxBufSize(2147483645)
    .withMaxCharBufSize(2147483645)

  val emptyStateAsString: String = writeToString(
    fromState(QueueImpl.State.empty)
  )

  def encode(state: QueueImpl.State): Array[Byte] =
    writeToArray(fromState(state))

  def decode(bytes: Array[Byte]): QueueImpl.State =
    readFromArray[SerializableQueueState](bytes, readerConfig).toState

  def decode(raw: String): QueueImpl.State =
    readFromString[SerializableQueueState](raw, readerConfig).toState
}
