package tasks.queue

case class Prefix(value: String) extends AnyVal

trait FilePrefix[A] {
  def prefix(a: A): Prefix
}

object FilePrefix {
  implicit def derivedFromSerializer[A](implicit
      ser: Serializer[A]
  ): FilePrefix[A] =
    new FilePrefix[A] {
      def prefix(a: A): Prefix = {
        val md = java.security.MessageDigest.getInstance("SHA-256")
        val digest = md.digest(ser(a))
        val sb = new java.lang.StringBuilder(digest.length * 2)
        var i = 0
        while (i < digest.length) {
          val b = digest(i) & 0xff
          sb.append(Character.forDigit(b >>> 4, 16))
          sb.append(Character.forDigit(b & 0xf, 16))
          i += 1
        }
        Prefix(sb.toString)
      }
    }
}
