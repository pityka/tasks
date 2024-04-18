package tasks.queue
import scala.quoted.*

@scala.annotation.experimental
object SerdeMacro {

  def create[A: Type](des: Expr[Deserializer[A]], ser: Expr[Serializer[A]])(
      using Quotes
  ): Expr[SerDe[A]] = {
    import quotes.reflect.*

    val name1 = Symbol
      .newVal(
        Symbol.spliceOwner,
        Symbol.freshName("serde"),
        TypeRepr.of[Spore[Unit, Deserializer[A]]],
        Flags.Final,
        Symbol.noSymbol
      )
      .tree
      .asExprOf[Spore[Unit, Deserializer[A]]]
    val name2 = Symbol
      .newVal(
        Symbol.spliceOwner,
        Symbol.freshName("serde"),
        TypeRepr.of[Spore[Unit, Serializer[A]]],
        Flags.Final,
        Symbol.noSymbol
      )
      .tree
      .asExprOf[Spore[Unit, Serializer[A]]]

    '{
      val $name1 = _root_.tasks.spore[Unit, Deserializer[A]] { (_: Unit) =>
        $des
      }
      val $name2 = _root_.tasks.spore[Unit, Serializer[A]] { (_: Unit) => $ser }
      _root_.tasks.queue.SerDe($name2, $name1)
    }

  }

}
