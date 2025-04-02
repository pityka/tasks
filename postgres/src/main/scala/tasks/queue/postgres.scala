package tasks.queue
import tasks.queue.QueueImpl._
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.LauncherName
import tasks.shared.VersionedResourceAllocated

import cats.effect._
import skunk._
import natchez.Trace.Implicits.noop
import skunk.util.Origin
import skunk.data.TransactionIsolationLevel
import skunk.data.TransactionAccessMode
import skunk.data.Completion
import tasks.elastic.NodeRegistryState
import tasks.util.message.Node
import tasks.shared.PendingJobId
import tasks.shared.RunningJobId

object Postgres {
  private[tasks] case class SerializableState(
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
      /*This is non empty while waiting for response from the tasklauncher
       *during that, no other tasks are started*/
      negotiation: Option[(LauncherName, ScheduleTask)],
      counters: List[(LauncherName, Long)],
      nodes: NodeRegistryState.State
  ) {
    def toState = QueueImpl.State(
      queuedTasks = queuedTasks.toMap,
      scheduledTasks = scheduledTasks.toMap,
      knownLaunchers = knownLaunchers.toMap,
      negotiation = negotiation,
      counters = counters.toMap,
      nodes = nodes
    )
  }
  private[tasks] object SerializableState {
    def fromState(state: QueueImpl.State) = SerializableState(
      queuedTasks = state.queuedTasks.toList,
      scheduledTasks = state.scheduledTasks.toList,
      knownLaunchers = state.knownLaunchers.toList,
      negotiation = state.negotiation,
      counters = state.counters.toList,
      nodes = state.nodes
    )
    import com.github.plokhotnyuk.jsoniter_scala.core._
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val keyCodec1: JsonKeyCodec[PendingJobId] =
      new JsonKeyCodec[PendingJobId] {
        def decodeKey(in: JsonReader): PendingJobId = PendingJobId(
          in.readKeyAsString()
        )
        def encodeKey(x: PendingJobId, out: JsonWriter): Unit = out.writeKey(x.value)
      }
    implicit val keyCodec2: JsonKeyCodec[RunningJobId] =
      new JsonKeyCodec[RunningJobId] {
        def decodeKey(in: JsonReader): RunningJobId = RunningJobId(
          in.readKeyAsString()
        )
        def encodeKey(x: RunningJobId, out: JsonWriter): Unit = out.writeKey(x.value)
      }
    implicit val codec: JsonValueCodec[SerializableState] = JsonCodecMaker.make
    val emptyStr = writeToString(fromState(State.empty))
  }

  def makeTransaction(
      table: String,
      host: String,
      port: Int,
      user: String,
      database: String,
      password: Option[String]
  ): Resource[IO, tasks.util.Transaction[tasks.queue.QueueImpl.State]] = Session
    .single[IO](
      host = host,
      port = port,
      user = user,
      database = database,
      password = password
    )
    .flatMap { session => makeTransaction(session, table) }

  def makeTransaction(
      session: Session[IO],
      table: String
  ): Resource[IO, tasks.util.Transaction[tasks.queue.QueueImpl.State]] = {
    assert(table.matches("[a-zA-Z0-9]+"), "table name must match [a-zA-Z0-9]+")
    import skunk.implicits._
    import skunk.codec.all._
    val prepare = session.transaction.use { _ =>
      val created = session
        .execute(
          Command(
            s"CREATE TABLE if not exists $table (value text)",
            Origin.unknown,
            Void.codec
          )
        )
      val count = session
        .unique(
          Query(
            s"select count(*) from $table",
            Origin.unknown,
            Void.codec,
            int8
          )
        )

      val insert = count.flatMap(i =>
        if (i == 0)
          session
            .prepare(
              Command(
                s"insert into $table values ($$1)",
                Origin.unknown,
                varchar
              )
            )
            .flatMap(_.execute(SerializableState.emptyStr))
        else IO.unit
      )

      created *> insert
    }
    Resource.eval(prepare.map(_ => new PostgresTransaction(session, table)))

  }

  private[tasks] class PostgresTransaction(
      session: Session[IO],
      table: String
  ) extends tasks.util.Transaction[tasks.queue.QueueImpl.State] {
    import skunk.implicits._
    import skunk.codec.all._
    import com.github.plokhotnyuk.jsoniter_scala.core._

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      val tx = session
        .transaction(
          TransactionIsolationLevel.Serializable,
          TransactionAccessMode.ReadWrite
        )
        .use { _ =>
          val io = get.flatMap { state =>
            val (updated, sideEffect) = update(state)
            val str = writeToString(SerializableState.fromState(updated))
            val command =
              Command(s"UPDATE $table SET value = $$1", Origin.unknown, text)
            session.prepare(command).flatMap(_.execute(str)).map { _ =>
              sideEffect
            }
          }
          io
        }
      def loop: IO[IO[B]] = tx.recoverWith {
        case SqlState.SerializationFailure(_) =>
          IO(
            scribe.info(
              s"Transaction failed to commit due to serialization failure (an other transaction in process). Try again."
            )
          ) *> loop
      }

      IO.uncancelable { poll =>
        poll(loop).flatten
      }
    }

    override def get: IO[State] = {
      val query = Query(
        s"SELECT value FROM $table limit 1",
        Origin.unknown,
        Void.codec,
        text
      )
      val raw = session.option(query)
      raw.map {
        case None => State.empty
        case Some(raw) =>
          val serializable = readFromString[SerializableState](raw)
          serializable.toState
      }
    }

  }

}
