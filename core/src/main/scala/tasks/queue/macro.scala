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

import tasks._
import scala.concurrent.Future

object Macros {
  import scala.reflect.macros.Context
  import scala.language.experimental.macros

  def asyncTaskDefinitionImpl[A: cxt.WeakTypeTag, C: cxt.WeakTypeTag](
      cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int]
  )(
      comp: cxt.Expr[A => ComputationEnvironment => Future[C]]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val c = weakTypeOf[C]
    val h =  {
      // evaluates the tree. taskID should not depend on runtime values
      val taskIDEval = cxt.eval(cxt.Expr[String](cxt.resetLocalAttrs(taskID.tree.duplicate)))
      TypeName(taskIDEval)
    }

    val t =
      tq"Function1[io.circe.Json,Function1[tasks.queue.ComputationEnvironment,scala.concurrent.Future[tasks.queue.UntypedResult]]]"
    val r = q"""
    class $h extends $t {
      val r = implicitly[io.circe.Decoder[$a]]
      val w = implicitly[io.circe.Encoder[$c]]
      val c = $comp
      def apply(j:io.circe.Json) =
          (ce:tasks.queue.ComputationEnvironment) => (c(r.decodeJson(j).right.get)(ce)).map(x => tasks.queue.UntypedResult.make(x)(w))(ce.executionContext)

    }
    new TaskDefinition[$a,$c](new $h,tasks.queue.TaskId($taskID,$taskVersion))
    """
    r
  }

}
