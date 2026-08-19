package tasks.util

private[tasks] object SessionId {

  private val separator = "~"

  def random: String = scala.util.Random.alphanumeric.take(16).mkString

  def tag(session: String, value: String): String =
    session + separator + value

  def of(value: String): Option[String] = {
    val i = value.indexOf(separator)
    if (i <= 0) None else Some(value.substring(0, i))
  }

  def belongsTo(value: String, session: String): Boolean =
    of(value).contains(session)

}
