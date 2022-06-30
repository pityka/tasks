package tasks.util.concurrent

import cats.effect._
import cats.instances.list._
import cats.syntax.all._

object NodeLocalCache {

  case class State[A](
      map: Map[
        String,
        (Option[((A, IO[Unit]), Int)], List[Deferred[IO, (A, IO[Unit])]])
      ]
  ) {}

  def make[A] = Ref.of[IO, State[A]](State(Map.empty))

  def offer[A](
      id: String,
      orElse: Resource[IO, A],
      stateR: Ref[IO, State[A]]
  ): Resource[IO, A] = {

    sealed trait Command
    case object YouShouldSetIt extends Command
    case class GiveFromCache(a: A) extends Command
    case object Wait extends Command

    def newOfferer(
        state: State[A],
        id: String,
        offerer: Deferred[IO, (A, IO[Unit])]
    ) =
      state.map.get(id) match {
        case None =>
          State(state.map.updated(id, (None, List(offerer)))) -> YouShouldSetIt

        case Some((Some((_, 0)), _)) =>
          throw new IllegalStateException(
            "Counter of a non-empty resource can't be zero."
          )
        case Some((Some((a, counter)), offerers)) =>
          State(state.map.updated(id, (Some((a, counter + 1)), offerers))) ->
            GiveFromCache(a._1)

        case Some((None, offerers)) =>
          (
            State(state.map.updated(id, (None, offerer :: offerers))),
            Wait
          )
      }

    val decrement: IO[Unit] =
      stateR.modify { case State(map) =>
        map.get(id) match {
          case None =>
            throw new IllegalStateException("Decrement never seen resource")
          case Some((None, _)) =>
            throw new IllegalStateException(
              "Decrement not yet computed resource"
            )
          case Some((Some((_, 1)), _ :: _)) =>
            throw new IllegalStateException(
              "Decrement a resource with a counter and waiting offerers."
            )
          case Some((Some(((_, release), 1)), Nil)) =>
            State(map.-(id)) -> release
          case Some((Some(((v, release), counter)), offerers)) =>
            State(
              map.updated(id, (Some(((v, release), counter - 1)), offerers))
            ) -> IO.unit
        }
      }.flatten

    val runAllocator =
      orElse.allocated.flatMap { case a @ (value, _) =>
        stateR.modify { case State(map) =>
          val (newState, list) = map.get(id) match {
            case None =>
              throw new IllegalStateException(
                "Running allocator on never seen resource"
              )
            case Some((Some(_), _)) =>
              throw new IllegalStateException(
                "Running allocator on already available resource"
              )
            case Some((None, offerers)) =>
              (
                State(
                  map.updated(id, (Some((a, offerers.size)), Nil))
                ),
                offerers
              )
          }
          val action: IO[Unit] =
            list.traverse(_.complete((value, decrement))).void
          (newState, action)
        }.flatten
      }

    val allocator = Deferred[IO, (A, IO[Unit])].flatMap { offerer =>
      Async[IO].uncancelable { _ => // `poll` used to embed cancelable code
        stateR
          .modify { state =>
            val (newState, command) = newOfferer(state, id, offerer)

            val action = command match {
              case YouShouldSetIt =>
                runAllocator
              case GiveFromCache(a) =>
                offerer.complete((a, decrement)).void
              case Wait => IO.unit
            }

            (newState, action)
          }
          .flatten
          .flatMap(_ => offerer.get)

      }

    }

    Resource(allocator)
  }
}
