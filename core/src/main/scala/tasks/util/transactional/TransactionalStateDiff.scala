package tasks.util.transactional

private[tasks] object TransactionalStateDiff {

  sealed trait Op[+P] {
    def key: String
  }

  final case class Insert[P](key: String, payload: P) extends Op[P]

  final case class Update[P](key: String, payload: P, baseVersion: Long)
      extends Op[P]

  final case class Delete(key: String, baseVersion: Long) extends Op[Nothing]

  final case class CheckVersion(key: String, baseVersion: Long)
      extends Op[Nothing]

  def diffCollection[P](
      oldItems: Map[String, P],
      newItems: Map[String, P],
      baseVersions: Map[String, Long],
      equal: (P, P) => Boolean
  ): List[Op[P]] = {
    val allKeys = (oldItems.keySet ++ newItems.keySet).toList.sorted
    allKeys.flatMap { k =>
      (oldItems.get(k), newItems.get(k)) match {
        case (Some(_), None) =>
          Some(Delete(k, baseVersions.getOrElse(k, 0L)))
        case (None, Some(p)) =>
          Some(Insert(k, p))
        case (Some(oldP), Some(newP)) if equal(oldP, newP) =>
          None
        case (Some(_), Some(newP)) =>
          Some(Update(k, newP, baseVersions.getOrElse(k, 0L)))
        case (None, None) =>
          None
      }
    }
  }

  def diffCollection[P](
      oldItems: Map[String, P],
      newItems: Map[String, P],
      baseVersions: Map[String, Long]
  ): List[Op[P]] =
    diffCollection(oldItems, newItems, baseVersions, (a: P, b: P) => a == b)

  def diffSingleton[P](
      key: String,
      oldItem: P,
      newItem: P,
      baseVersion: Long,
      equal: (P, P) => Boolean
  ): Option[Op[P]] =
    if (equal(oldItem, newItem)) None
    else Some(Update(key, newItem, baseVersion))

  def diffSingleton[P](
      key: String,
      oldItem: P,
      newItem: P,
      baseVersion: Long
  ): Option[Op[P]] =
    diffSingleton(key, oldItem, newItem, baseVersion, (a: P, b: P) => a == b)

  final case class WriteUnit[O](ops: List[O], bytes: Int)

  class OversizedUnitException(message: String)
      extends RuntimeException(message)

  def packUnits[O](
      units: List[WriteUnit[O]],
      maxOpsPerChunk: Int,
      maxBytesPerChunk: Int,
      describeOversized: WriteUnit[O] => String
  ): List[List[O]] = {
    units.foreach { u =>
      if (u.bytes > maxBytesPerChunk)
        throw new OversizedUnitException(describeOversized(u))
    }
    val builder = scala.collection.mutable.ListBuffer.empty[List[O]]
    var current = List.empty[O]
    var currentBytes = 0
    units.foreach { case WriteUnit(unitOps, unitBytes) =>
      val newSize = current.size + unitOps.size
      val newBytes = currentBytes + unitBytes
      if (
        current.nonEmpty &&
        (newSize > maxOpsPerChunk || newBytes > maxBytesPerChunk)
      ) {
        builder += current
        current = unitOps
        currentBytes = unitBytes
      } else {
        current = current ++ unitOps
        currentBytes = newBytes
      }
    }
    if (current.nonEmpty) builder += current
    builder.toList
  }

  def packUnits[O](
      units: List[WriteUnit[O]],
      maxOpsPerChunk: Int,
      maxBytesPerChunk: Int
  ): List[List[O]] =
    packUnits(
      units,
      maxOpsPerChunk,
      maxBytesPerChunk,
      (u: WriteUnit[O]) =>
        s"write unit of ${u.bytes} bytes exceeds the per-transaction limit of $maxBytesPerChunk bytes"
    )

  def withSingletonBarrier[O](
      dataChunks: List[List[O]],
      barrier: O,
      terminalSingletonOp: Option[O]
  ): List[List[O]] =
    (terminalSingletonOp, dataChunks) match {
      case (None, Nil)    => Nil
      case (Some(t), Nil) => List(List(t))
      case (None, chunks) => chunks.map(_ :+ barrier)
      case (Some(t), chunks) =>
        val init = chunks.init
        val last = chunks.last
        init.map(_ :+ barrier) :+ (last :+ t)
    }

}
