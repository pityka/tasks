package tasks.queue
import tasks.queue.QueueImpl._

import cats.effect._
import skunk._
import natchez.Trace.Implicits.noop
import skunk.util.Origin
import skunk.data.TransactionIsolationLevel
import skunk.data.TransactionAccessMode

object Postgres {

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
            .flatMap(_.execute(SerializableQueueState.emptyStateAsString))
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

    override def flatModify[B](update: State => (State, IO[B])): IO[B] = {
      val tx = session
        .transaction(
          TransactionIsolationLevel.Serializable,
          TransactionAccessMode.ReadWrite
        )
        .use { _ =>
          val io = get.flatMap { state =>
            val (updated, sideEffect) = update(state)
            val str = new String(
              SerializableQueueState.encode(updated),
              java.nio.charset.StandardCharsets.UTF_8
            )
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
        case None      => State.empty
        case Some(raw) => SerializableQueueState.decode(raw)
      }
    }

  }

}
