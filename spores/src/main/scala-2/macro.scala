/*
 * The MIT License
 *
 * Copyright (c) 2019 Istvan Bartha
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



object SporeMacros {
  import scala.reflect.macros.blackbox.Context

  def failIfInner(cxt: Context) = {
    import cxt.universe._
    val noOuterClass = (cxt.reifyEnclosingRuntimeClass match {
      case q"this.getClass" => true
      case _                => false
    })
    if (!noOuterClass) {
      cxt.abort(
        cxt.enclosingPosition,
        "spore must not be defined in an inner class"
      )
    }
  }

  def sporeImpl0[B: cxt.WeakTypeTag](cxt: Context)(
      value: cxt.Expr[B]
  ) = {
    import cxt.universe._
    val name = cxt.freshName(TypeName("spore0"))
    val instance = cxt.freshName(TermName("sporeInstance0"))
    val unitName = cxt.freshName(TermName("sporeName0"))
    val b = weakTypeOf[B]

    val (editedFn, capturedExternalReferences) =
      SporeAnalysis.ensureNoExternalReferences(cxt)(value.tree)

    val (_, editedBody) = editedFn match {
      case Function(params, body) =>
        (params, body)
      case _ => cxt.abort(value.tree.pos, "Expected function literal.")
    }

    val (paramList, externalReferences) = capturedExternalReferences.unzip

    val validParamList = (paramList zip externalReferences).map {
      case (name, symbol) =>
        q"val $name: ${symbol.info}"
    }

    val t = q"""
    final class $name(..$validParamList) extends _root_.tasks.queue.SporeFun[scala.Unit,$b] {
      def call($unitName: _root_.scala.Unit) : $b = {
        val _ = $unitName
        $editedBody
      }
    }
    val $instance = new $name(..${externalReferences.map(_.name)})
    _root_.tasks.queue.Spore[_root_.scala.Unit,$b]($instance.getClass.getName,Seq(..${externalReferences
      .map(_.name)}))
    """
    failIfInner(cxt)

    t
  }
  def sporeImpl[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](cxt: Context)(
      value: cxt.Expr[A => B]
  ) = {
    import cxt.universe._
    val name = cxt.freshName(TypeName("spore"))
    val instance = cxt.freshName(TermName("sporeInstance"))
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val (editedFn, capturedExternalReferences) =
      SporeAnalysis.ensureNoExternalReferences(cxt)(value.tree.duplicate)

    val (editedParams, editedBody) = editedFn match {
      case Function(params, body) =>
        (params, body)
      case _ => cxt.abort(value.tree.pos, "Expected function literal.")
    }

    val (paramList, externalReferences) = capturedExternalReferences.unzip

    val validParamList = (paramList zip externalReferences).map {
      case (name, symbol) =>
        q"val $name: ${symbol.info}"
    }

    val t = q"""
    final class $name(..$validParamList) extends _root_.tasks.queue.SporeFun[$a,$b] {
      def call(..$editedParams) : $b = $editedBody
    }
    val $instance = new $name(..${externalReferences.map(_.name)})
    _root_.tasks.queue.Spore[$a,$b]($instance.getClass.getName,Seq(..${externalReferences
      .map(_.name)}))
    """
    failIfInner(cxt)
    t
  }

}
