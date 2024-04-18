package tasks.queue
import scala.quoted.*
import scala.annotation.experimental

@experimental
object SporeMacros:

  def failIfInner(using Quotes) = {
    import quotes.reflect.*

    def isInClass(symbol: Symbol): Boolean = {
      if (symbol.isNoSymbol) false
      else if (
        (symbol.isClassDef && symbol != symbol.moduleClass) ||
        symbol.isAnonymousClass
      ) {
        true
      } else isInClass(symbol.owner)
    }

    if (isInClass(Symbol.spliceOwner))
      report.errorAndAbort(
        s"${Symbol.spliceOwner.fullName} is in a class. "
      )
    else ()
  }

  inline def spore[A, B](value: A => B): Spore[A, B] = ${
    sporeImpl[A, B]('value)
  }

  def sporeImpl[A: Type, B: Type](
      value: Expr[A => B]
  )(using Quotes): Expr[Spore[A, B]] = {

    failIfInner

    import quotes.reflect.*
    // report.errorAndAbort('{implicitly[Ordering[Double]]}.asTerm.show)

    // decompose the lambda to parameter list and body
    val (lam, params, body) = value.asTerm match {
      case Inlined(_, _, ident @ Ident(_)) =>
        ident.symbol.tree match {
          case ValDef(_, _, Some(Block(Nil, lam @ Lambda(params, body)))) =>
            (lam, params, body)
          case ValDef(_, _, Some(lam @ Lambda(params, body))) =>
            (lam, params, body)
          case _ =>
            throw new MatchError(
              s"Expected lambda, got: ${ident.symbol.tree} \n ${ident.symbol.tree.show}"
            )
        }

      case _ =>
        throw new MatchError(
          s"Expected lambda, got: ${value.asTerm}"
        )
    }
    if (params.size != 1) {
      report.errorAndAbort(s"Expected lambda with 1 parameter got: $params")
    }
    def isOwner(sym: Symbol, owner: Symbol): Boolean = {
      sym != null && (sym.owner == owner || {
        !sym.owner.isNoSymbol && isOwner(sym.owner, owner)
      })
    }

    def isSymbolChildOfSpore(childSym: Symbol) =
      isOwner(childSym, lam.symbol)

    def isStatic(symbol: Symbol): Boolean = {
      if (symbol.isNoSymbol) true
      else if (
        (symbol.isClassDef && symbol != symbol.moduleClass) ||
        symbol.isDefDef ||
        symbol.isAnonymousFunction ||
        symbol.isAnonymousClass
      ) false
      else isStatic(symbol.owner)
    }

    def isSymbolValid(s: Symbol): Boolean = {
      isSymbolChildOfSpore(s) ||
      isStatic(s)
    }

    def symbolIsSpore(sym: Symbol): Boolean = {
      sym.info match {
        case t: AppliedType =>
          t.tycon =:= TypeRepr.of[Spore[?, ?]].asInstanceOf[AppliedType].tycon

        case _ =>
          false
      }
    }

    object FindReferencesToSpores
        extends quotes.reflect.TreeAccumulator[List[(Tree, Symbol)]] {
      override def foldTree(
          foundReferencesToSpores: List[(Tree, Symbol)],
          tree: Tree
      )(owner: Symbol): List[(Tree, Symbol)] = {
        tree match {

          case Ident(_) | This(_) | Super(_, _) =>
            val sym = tree.symbol
            if (
              (!sym.isNoSymbol && !isSymbolValid(tree.symbol)) || symbolIsSpore(
                sym
              )
            ) {
              if (!symbolIsSpore(sym)) {
                report.errorAndAbort(
                  s"Reference to an invalid symbol: $sym.",
                  tree.pos
                )

              } else if (params.head.symbol == tree.symbol)
                foundReferencesToSpores
              else {
                // report.info(
                //   s"Found references to spore within spore",
                //   tree.asExpr
                // )
                (tree, tree.symbol) :: foundReferencesToSpores
              }
            } else foundReferencesToSpores

          case sym =>
            foldOverTree(foundReferencesToSpores, tree)(owner)
        }
      }
    }

    val (referencesToSporesAsTrees, referencesToSpores) =
      FindReferencesToSpores.foldOverTree(Nil, body)(body.symbol).unzip

    // a tree traverser which replaces each reference of the parameter to a new identifier
    def replaceParam(param: Term) = new quotes.reflect.TreeMap {
      override def transformTerm(tree: Term)(owner: Symbol): Term =
        tree match {
          case i @ Ident(_) if i.symbol == params.head.symbol =>
            param
          case _ => super.transformTerm(tree)(owner)
        }
    }

    def replaceReferencesToSpores(
        body: Term,
        sequenceParam: Term
    ) = {
      val traverse = new quotes.reflect.TreeMap {

        override def transformTerm(tree: Term)(owner: Symbol): Term =
          tree match {
            case Ident(_)
                if symbolIsSpore(tree.symbol) && referencesToSpores.contains(
                  tree.symbol
                ) =>
              val idx = referencesToSpores.indexOf(tree.symbol)
              val tree2 = '{
                ${ sequenceParam.asExprOf[Seq[Spore[Any, Any]]] }.apply(${
                  Expr(idx)
                })
              }
              Select
                .unique(tree2.asTerm, "asInstanceOf")
                .appliedToType(tree.symbol.info)
            case _ =>
              super.transformTerm(tree)(owner)
          }
      }
      val rewritten = traverse.transformTerm(body)(value.asTerm.symbol)
      rewritten
    }

    /** Scala3 incantations for:
      *
      * class fresh extend SporeFun[A,B] { def call(dependencies,param) = body
      * }; new fresh()
      */
    val sporeFunInstance = {
      // extend Object with SporeFun[A,B]
      val parents = List(
        TypeTree.of[Object],
        TypeTree.of[SporeFunWithDependencies[A, B]]
      )
      def declSymbol(clsSymbol: Symbol) = List(
        {
          Symbol.newMethod(
            clsSymbol,
            "call",
            MethodType(List("dependencies", params.head.name))(
              (_: MethodType) =>
                List(TypeRepr.of[Seq[Spore[Any, Any]]], TypeRepr.of[A]),
              (_: MethodType) => TypeRepr.of[B]
            )
          )

        }
      )
      def defdef(sym: Symbol) = DefDef(
        sym,
        (list) => {
          val p0 = list.head.head match {
            case t: Term => t
            case x =>
              throw MatchError(
                "Expected term. Got: " + x.show(using Printer.TreeStructure)
              )
          }
          val p1 = list.head(1) match {
            case t: Term => t
            case x =>
              throw MatchError(
                "Expected term. Got in second param: " + x.show(using
                  Printer.TreeStructure
                )
              )
          }

          Some(
            replaceReferencesToSpores(
              replaceParam(p1).transformTerm(body.changeOwner(sym))(sym),
              p0
            )
          )
        }
      )

      val cls = Symbol.newClass(
        Symbol.spliceOwner,
        Symbol.freshName("sporefun"),
        parents.map(_.tpe),
        declSymbol,
        None
      )
      def callSymbol = cls.declaredMethod("call").head
      val clsDef = ClassDef(
        cls,
        parents,
        List(defdef(callSymbol))
      )
      val newCls = Typed(
        Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil),
        TypeTree.of[SporeFunWithDependencies[A, B]]
      )
      Block(List(clsDef), newCls).asExprOf[SporeFunWithDependencies[A, B]]

    }
    val expr = '{
      {
        val instance = ${ sporeFunInstance }
        Spore[A, B](
          instance.getClass.getName,
          ${
            Expr.ofList(referencesToSporesAsTrees.map(_.asExprOf[Any]))
          }.asInstanceOf[Seq[Spore[Any, Any]]]
        )
      }
    }

    // report.info(s"Rewriting spore body to ${expr.show}")

    (expr)

  }
