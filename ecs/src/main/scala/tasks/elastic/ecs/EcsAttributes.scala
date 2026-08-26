package tasks.elastic.ecs

import tasks.shared.NodeSelector

sealed trait InstanceConstraint

object InstanceConstraint {
  case object EveryInstance extends InstanceConstraint
  case object NoInstance extends InstanceConstraint
  final case class Expression(text: String) extends InstanceConstraint
}

object EcsAttributes {

  val maximumAttributeLength: Int = 128

  private val attributeName = """[A-Za-z0-9_./\\-]+""".r

  private val attributeValue = """[A-Za-z0-9_.@/\\:-]+""".r

  private val separator = ':'

  def labelOf(name: String, value: Option[String]): String =
    value.filter(_.nonEmpty).fold(name)(v => s"$name$separator$v")

  def labelOf(name: String, value: String): String =
    labelOf(name, Option(value))

  def labelOf(name: String): String =
    labelOf(name, Option.empty[String])

  def attributeOf(label: String): Either[String, (String, Option[String])] = {
    val (name, value) = label.indexOf(separator.toInt) match {
      case -1    => (label, Option.empty[String])
      case index => (label.take(index), Some(label.drop(index + 1)))
    }

    def malformed(what: String) =
      Left(
        s"The node selector label '$label' cannot be expressed as an ECS " +
          s"container instance attribute: $what. A label is either " +
          "'name:value' or a bare 'name'. Attribute names accept letters, " +
          "digits, underscores, periods, slashes and hyphens; values " +
          "additionally accept at signs and colons, but no spaces. Both are " +
          s"limited to $maximumAttributeLength characters."
      )

    if (!attributeName.matches(name))
      malformed(s"'$name' is not a valid attribute name")
    else if (name.length > maximumAttributeLength)
      malformed(s"the attribute name '$name' is too long")
    else
      value match {
        case None => Right((name, None))
        case Some(v) if !attributeValue.matches(v) =>
          malformed(s"'$v' is not a valid attribute value")
        case Some(v) if v.length > maximumAttributeLength =>
          malformed(s"the attribute value '$v' is too long")
        case Some(v) => Right((name, Some(v)))
      }
  }

  def constraintOf(selector: NodeSelector): Either[String, InstanceConstraint] =
    selector match {
      case NodeSelector.Always => Right(InstanceConstraint.EveryInstance)

      case NodeSelector.Has(label) =>
        attributeOf(label).map {
          case (name, None) =>
            InstanceConstraint.Expression(s"attribute:$name exists")
          case (name, Some(value)) =>
            InstanceConstraint.Expression(s"attribute:$name == $value")
        }

      case NodeSelector.Not(inner) =>
        constraintOf(inner).map {
          case InstanceConstraint.EveryInstance => InstanceConstraint.NoInstance
          case InstanceConstraint.NoInstance => InstanceConstraint.EveryInstance
          case InstanceConstraint.Expression(text) =>
            InstanceConstraint.Expression(s"not($text)")
        }

      case NodeSelector.And(selectors) =>
        sequence(selectors).map { constraints =>
          if (constraints.contains(InstanceConstraint.NoInstance))
            InstanceConstraint.NoInstance
          else
            join(constraints, "and", InstanceConstraint.EveryInstance)
        }

      case NodeSelector.Or(selectors) =>
        sequence(selectors).map { constraints =>
          if (constraints.contains(InstanceConstraint.EveryInstance))
            InstanceConstraint.EveryInstance
          else join(constraints, "or", InstanceConstraint.NoInstance)
        }
    }

  def placementExpression(
      selector: Option[NodeSelector]
  ): Either[String, Option[String]] =
    selector match {
      case None => Right(None)
      case Some(s) =>
        constraintOf(s).flatMap {
          case InstanceConstraint.EveryInstance    => Right(None)
          case InstanceConstraint.Expression(text) => Right(Some(text))
          case InstanceConstraint.NoInstance =>
            Left(
              s"The node selector $s can never be satisfied by any ECS " +
                "container instance, so no worker task was placed for it. " +
                "An empty Or, or a Not of a selector which matches every " +
                "node, has no instance that could run the task."
            )
        }
    }

  private def sequence(
      selectors: List[NodeSelector]
  ): Either[String, List[InstanceConstraint]] =
    selectors.foldLeft(
      Right(Nil): Either[String, List[InstanceConstraint]]
    ) { (acc, selector) =>
      acc.flatMap(done => constraintOf(selector).map(done :+ _))
    }

  private def join(
      constraints: List[InstanceConstraint],
      operator: String,
      whenEmpty: InstanceConstraint
  ): InstanceConstraint = {
    val expressions = constraints.collect {
      case InstanceConstraint.Expression(text) => text
    }
    if (expressions.isEmpty) whenEmpty
    else if (expressions.size == 1)
      InstanceConstraint.Expression(expressions.head)
    else
      InstanceConstraint.Expression(
        expressions.map(text => s"($text)").mkString(s" $operator ")
      )
  }
}
