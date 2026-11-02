package tasks.util

private[tasks] case class CorrelationId(l: Long)
private[tasks] object CorrelationId {
  def make = CorrelationId(scala.util.Random.nextLong())
  implicit def toLogFeature(rm: CorrelationId): scribe.LogFeature = scribe.data(
    Map(
      "log-correlation" -> rm.l
    )
  )
}
