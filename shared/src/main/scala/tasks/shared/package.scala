package tasks

package object shared {

  sealed trait CodeVersion { def s: String }
  object CodeVersion {
    private[tasks] final case class CodeVersionImpl(s: String)
        extends CodeVersion
    def apply(s: String): CodeVersion = CodeVersionImpl(s)
    def unapply(v: CodeVersion): Some[String] = Some(v.s)
  }

  sealed trait Priority { def s: Int }
  object Priority {
    private[tasks] final case class PriorityImpl(s: Int) extends Priority
    def apply(s: Int): Priority = PriorityImpl(s)
    def unapply(p: Priority): Some[Int] = Some(p.s)
  }

  private[tasks] case class ElapsedTimeNanoSeconds(s: Long)

}
