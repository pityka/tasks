package tasks.util

import cats.effect.IO
import cats.effect.kernel.Ref

trait Transaction[T] {
  def flatModify[B](update: T => (T, IO[B])): IO[B]
  def update(update: T => T) = flatModify(t => (update(t), IO.unit))
  def get: IO[T]
}

object Transaction {
  def fromRef[T](ref: Ref[IO, T]) = new Transaction[T] {
    def get: IO[T] = ref.get
    def flatModify[B](update: T => (T, IO[B])): IO[B] = ref.flatModify(update)
  }
}
