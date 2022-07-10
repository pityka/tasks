package tasks.util

/** Holds a hostname and a port 
 * 
 * No name resolution. 
 */
case class SimpleSocketAddress(hostName: String, port: Int) {
  def getHostName = hostName 
  def getPort = port
}