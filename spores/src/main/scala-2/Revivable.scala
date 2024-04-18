/*
 * The MIT License
 *
 * Copyright (c) 2019 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package tasks.queue

trait SporeFun[A, B] {
  def call(a: A): B
}

trait Revivable[A, B] {

  def fqcn: String
  def dependencies: Seq[Spore[Any, Any]]

  def revive: SporeFun[A, B] = {
    try {
      val ctors = java.lang.Class
        .forName(fqcn)
        .getConstructors()

      val selectedCtor =
        ctors.find(_.getParameterCount == dependencies.size).get

      selectedCtor
        .newInstance(dependencies: _*)
        .asInstanceOf[tasks.queue.SporeFun[A, B]]
    } catch {
      case e: java.lang.NoSuchMethodException =>
        throw SporeException(
          s"Available ctors: ${java.lang.Class
              .forName(fqcn)
              .getConstructors()
              .toList}",
          e
        )
    }
  }

}
