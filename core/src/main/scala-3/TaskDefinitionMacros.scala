/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.queue
import cats.effect.IO

import scala.quoted.*

@scala.annotation.experimental
object TaskDefinitionMacros {

  //  def sporeImpl[A: Type, B: Type](
  //     value: Expr[A => B]
  // )(using Quotes): Expr[Spore[A, B]] = {

  def taskDefinitionFromTree[A: Type, C: Type](
      serA: Expr[Serializer[A]],
      deserA: Expr[Deserializer[A]],
      serC: Expr[Serializer[C]],
      deserC: Expr[Deserializer[C]],
      taskID: Expr[String],
      taskVersion: Expr[Int],
      comp: Expr[A => ComputationEnvironment => IO[C]]
  )(using Quotes) = {
    import quotes.reflect.*

    def valdef[T: Type](expr: Expr[T]) = {
      val name1 = Symbol.freshName("task")
      val symbol1 = Symbol.newVal(
        Symbol.spliceOwner,
        name1,
        TypeRepr.of[T],
        Flags.Final,
        Symbol.noSymbol
      )
      val def1 = ValDef(symbol1, Some(expr.asTerm.changeOwner(symbol1)))

      val ident1 = Ref(symbol1)

      (def1, ident1.asExprOf[T])
    }

    val (d1, i1) = valdef('{ _root_.tasks.spore { (_: Unit) => $deserA } })
    val (d2, i2) = valdef('{ _root_.tasks.spore { (_: Unit) => $serC } })
    val (d3, i3) = valdef(tasks.queue.SporeMacros.sporeImpl(comp))

    val td = '{
      _root_.tasks.TaskDefinition[A, C](
        ${ i1 },
        $i2,
        $i3,
        _root_.tasks.queue.TaskId($taskID, $taskVersion)
      )($serA, $deserC)
    }

    val block = Block(List(d1, d2, d3), td.asTerm)
      .asExprOf[_root_.tasks.TaskDefinition[A, C]]

    // report.info(s"Expanded task def to \n${block.asTerm.show}")

    block

  }

}
