/**
  * This code is a modification of the code found at https://github.com/scalacenter/spores
  * That scalacenter/spores is licensed under the SCALA license which is reproduced below.
  *
  * SCALA LICENSE
  *
  * Copyright (c) 2002-2012 EPFL, Lausanne, unless otherwise specified.
  * All rights reserved.
  *
  * This software was developed by the Programming Methods Laboratory of the
  * Swiss Federal Institute of Technology (EPFL), Lausanne, Switzerland.
  *
  * Permission to use, copy, modify, and distribute this software in source
  * or binary form for any purpose with or without fee is hereby granted,
  * provided that the following conditions are met:
  *
  *    1. Redistributions of source code must retain the above copyright
  *       notice, this list of conditions and the following disclaimer.
  *
  *    2. Redistributions in binary form must reproduce the above copyright
  *       notice, this list of conditions and the following disclaimer in the
  *       documentation and/or other materials provided with the distribution.
  *
  *    3. Neither the name of the EPFL nor the names of its contributors
  *       may be used to endorse or promote products derived from this
  *       software without specific prior written permission.
  *
  *
  * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
  * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
  * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
  * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
  * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
  * SUCH DAMAGE.
  */
package tasks.queue

import scala.reflect.macros.blackbox.Context

object SporeAnalysis {

  def ensureNoExternalReferences(cxt: Context)(tree: cxt.Tree) = {
    import cxt.universe._

    def readSporeFunDef(tree: Tree): (Option[Function], List[Tree], Tree) = {
      tree match {
        case f @ Function(params, body) =>
          (Some(f), params, body)
        case _ => cxt.abort(tree.pos, "Expected function literal.")
      }
    }

    val (fun, params, body) = readSporeFunDef(tree)

    val funSymbol = fun.map(_.symbol)

    def isOwner(sym: Symbol, owner: Symbol): Boolean = {
      sym != null && (sym.owner == owner || {
        sym.owner != NoSymbol && isOwner(sym.owner, owner)
      })
    }

    def isSymbolChildOfSpore(childSym: Symbol) =
      funSymbol.exists(sym => isOwner(childSym, sym.asInstanceOf[Symbol]))

    def isStaticPath(s: Symbol): Boolean = {
      s != NoSymbol && {
        (s.isMethod && isStaticPath(s.owner)) || {
          (s.isModule || s.isModuleClass || s.isPackage || s.isPackageClass) &&
          (s.isStatic || isStaticPath(s.owner))
        }
      }
    }

    def isSymbolValid(s: Symbol): Boolean = {
      isSymbolChildOfSpore(s) ||
      isStaticPath(s)
    }

    val sporeType = typeOf[Spore[_, _]]
    def isSymbolSpore(sym: Symbol): Boolean =
      sym.info.erasure =:= sporeType

    object ReferenceInspector extends Traverser {
      var foundReferencesToSpores = List.empty[Symbol]
      override def traverse(tree: Tree): Unit = {
        tree match {
          case New(_) =>
          case Ident(_) | This(_) | Super(_, _) =>
            val sym = tree.symbol
            if (sym != NoSymbol && !isSymbolValid(tree.symbol)) {
              if (!isSymbolSpore(sym)) {
                cxt.abort(tree.pos, s"Reference to an invalid symbol: $sym.")
              } else {
                foundReferencesToSpores = tree.symbol :: foundReferencesToSpores
              }
            }

          case _ =>
            super.traverse(tree)
        }
      }
    }

    class ReplaceSpores(
        mappingFromOldToNewNames: Map[Symbol, Ident],
        invalidSymbols: List[Symbol]
    ) extends Transformer {
      var capturedSpores = List.empty[(TermName, Symbol)]
      override def transform(tree: Tree): Tree = {
        tree match {
          case Ident(_)
              if isSymbolSpore(tree.symbol) && invalidSymbols.contains(
                tree.symbol
              ) =>
            val sym = tree.symbol
            val freshName = cxt.freshName(TermName("t"))
            capturedSpores = (freshName, sym) :: capturedSpores
            q"""$freshName"""

          case Ident(_) => mappingFromOldToNewNames.getOrElse(tree.symbol, tree)

          case _ => super.transform(tree)
        }
      }
    }

    def checkReferencesInBody(sporeBody: Tree) =
      ReferenceInspector.traverse(sporeBody)

    val paramMods = Modifiers(Flag.PARAM)
    val paramTermName = TermName("s")

    def generateNewParameters(syms: List[Symbol]) = {
      def defineParam(name: TermName, sym: Symbol): ValDef =
        ValDef(paramMods, name, TypeTree(sym.typeSignature), EmptyTree)
      val paramNames = syms.map(_ => cxt.freshName(paramTermName))
      val references = paramNames.map(pn => q"$pn")
      val valDefs = paramNames.zip(syms).map(t => defineParam(t._1, t._2))
      valDefs -> references
    }

    def rewrite(
        oldParamSymbols: List[Symbol],
        oldBody: Tree,
        invalidSymbols: List[Symbol]
    ) = {

      val (newParamDefs, newParamRefs) = generateNewParameters(oldParamSymbols)
      val mapping = oldParamSymbols.zip(newParamRefs).toMap
      val rewriter = new ReplaceSpores(mapping, invalidSymbols)
      val body = cxt.untypecheck(rewriter.transform(oldBody))

      (Function(newParamDefs, body), rewriter.capturedSpores)
    }

    checkReferencesInBody(body)

    val (rewritten, capturedSporeSymbols) = rewrite(
      params.map(_.symbol),
      body,
      ReferenceInspector.foundReferencesToSpores
    )
    (rewritten, capturedSporeSymbols)

  }
}
