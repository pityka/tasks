package tasks

sealed trait AllowEmptyInput

object unsafe {
  implicit val allowEmptyInput: AllowEmptyInput = new AllowEmptyInput {}
}
