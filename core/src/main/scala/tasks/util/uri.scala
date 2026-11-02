/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.util

sealed trait Uri {
  def uri: String
  private[tasks] final def jUri = new java.net.URI(uri)
  private[tasks] final def http4s = org.http4s.Uri.fromString(uri).toOption.get
  override def toString = uri
  private[tasks] final def scheme =
    if (jUri.getScheme == null) "file" else jUri.getScheme
  private[tasks] final def authority = jUri.getAuthority
  private[tasks] final def path = jUri.getPath
}

object Uri {

  private[tasks] final case class UriImpl(uri: String) extends Uri

  def apply(uri: String): Uri = UriImpl(uri)

  def apply(scheme: String, hostname: String, port: Int, path: String): Uri =
    UriImpl(s"${scheme}://${hostname}:${port}$path")

  def unapply(u: Uri): Some[String] = Some(u.uri)

  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import com.github.plokhotnyuk.jsoniter_scala.core._

  implicit val codec: JsonValueCodec[Uri] = {
    val innerCodec: JsonValueCodec[UriImpl] = JsonCodecMaker.make
    new JsonValueCodec[Uri] {
      val nullValue: Uri = null
      def encodeValue(x: Uri, out: JsonWriter): Unit =
        innerCodec.encodeValue(x.asInstanceOf[UriImpl], out)
      def decodeValue(in: JsonReader, default: Uri): Uri =
        innerCodec.decodeValue(in, null)
    }
  }

}
