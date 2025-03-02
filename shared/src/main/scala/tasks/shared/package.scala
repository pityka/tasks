package tasks

import com.github.plokhotnyuk.jsoniter_scala.core._

package object shared {

  case class CodeVersion(s: String)

  case class Priority(s: Int)

  case class ElapsedTimeNanoSeconds(s: Long)

}
