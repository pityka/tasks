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

private[tasks] object TaskDefinitionMacros {
  import scala.reflect.macros.blackbox.Context

  def taskDefinitionFromTree[A: cxt.WeakTypeTag, C: cxt.WeakTypeTag](
      cxt: Context
  )(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int]
  )(
      comp: cxt.Expr[A => ComputationEnvironment => IO[C]]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val c = weakTypeOf[C]
    val name1 = cxt.freshName(TermName("task"))
    val name2 = cxt.freshName(TermName("task"))
    val name3 = cxt.freshName(TermName("task"))

    val r = q"""
      val $name1 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Deserializer[$a]]}
      val $name2 = _root_.tasks.spore{() => implicitly[_root_.tasks.queue.Serializer[$c]]}
      val $name3 = _root_.tasks.spore{$comp}
      new _root_.tasks.TaskDefinition[$a,$c]($name1,$name2,$name3,_root_.tasks.queue.TaskId($taskID,$taskVersion))
    """
    r
  }

}
