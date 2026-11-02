package tasks.util

/** Holds a hostname and a port
  *
  * No name resolution.
  */
sealed trait SimpleSocketAddress {
  def hostName: String
  def port: Int
  final def getHostName = hostName
  final def getPort = port
}

object SimpleSocketAddress {
  private[tasks] final case class SimpleSocketAddressImpl(
      hostName: String,
      port: Int
  ) extends SimpleSocketAddress

  def apply(hostName: String, port: Int): SimpleSocketAddress =
    SimpleSocketAddressImpl(hostName, port)

  def unapply(s: SimpleSocketAddress): Some[(String, Int)] =
    Some((s.hostName, s.port))
}
