/*
 * Copyright 2013 Heiko Seeberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified package names
 */

package tasks.util

/**
  * Type safe equality http://hseeberger.github.io/blog/2013/05/31/implicits-unchained-type-safe-equality-part2/
  */
object eq {

  implicit class Equal[L](val left: L) extends AnyVal {
    def ===[R](right: R)(implicit equality: Equality[L, R]): Boolean =
      equality.areEqual(left, right)
    def !==[R](right: R)(implicit equality: Equality[L, R]): Boolean =
      equality.areNotEqual(left, right)
  }
  @scala.annotation.implicitNotFound(
      "TypeWiseBalancedEquality requires ${L} and ${R} to be in a subtype relationship!")
  trait Equality[L, R] {
    def areEqual(left: L, right: R): Boolean
    def areNotEqual(left: L, right: R): Boolean
  }
  object Equality extends LowPriorityEqualityImplicits {
    implicit def rightSubtypeOfLeftEquality[L, R <: L]: Equality[L, R] =
      AnyEquality.asInstanceOf[Equality[L, R]]
  }
  trait LowPriorityEqualityImplicits {
    implicit def leftSubtypeOfRightEquality[R, L <: R]: Equality[L, R] =
      AnyEquality.asInstanceOf[Equality[L, R]]
  }
  private object AnyEquality extends Equality[Any, Any] {
    override def areEqual(left: Any, right: Any): Boolean = left == right
    override def areNotEqual(left: Any, right: Any): Boolean = left != right
  }

  /* 'Type-safe' contains.*/
  class SeqWithHas[T](s: Seq[T]) {
    def has(t: T) = s.contains(t)
  }
  implicit def seq2seqwithhas[T](s: Seq[T]) = new SeqWithHas(s)
}
