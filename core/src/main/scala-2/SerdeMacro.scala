package tasks.queue
object SerdeMacro {
  import scala.reflect.macros.blackbox.Context

  def create[A: cxt.WeakTypeTag](cxt: Context) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val name1 = cxt.freshName(TermName("serde"))
    val name2 = cxt.freshName(TermName("serde"))

    val r = q"""
      val $name1 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Deserializer[$a]]}
      val $name2 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Serializer[$a]]}
      _root_.tasks.queue.SerDe($name2,$name1)
    """
    r
  }

}
