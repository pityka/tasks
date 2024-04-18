package tasks
private[tasks] object tag {
  def apply[U] = Tagger.asInstanceOf[Tagger[U]]

  trait Tagged[U] extends Any
  type @@[+T, U] = T with Tagged[U]

  class Tagger[U] {
    def apply[T](t: T): T @@ U = t.asInstanceOf[T @@ U]
  }
  private object Tagger extends Tagger[Nothing]
}
