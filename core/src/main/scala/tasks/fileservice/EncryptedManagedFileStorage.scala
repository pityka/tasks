/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha


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

package tasks.fileservice

import tasks.util._
import tasks.util.config._
import tasks.util.eq._

import java.io.File

import cats.effect.kernel.Resource
import cats.effect.IO
import fs2.{Stream, Pipe}
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import tasks.fileservice._
import cats.effect.std.SecureRandom

class EncryptedManagedFileStorage(
    parent: ManagedFileStorage,
    keyHex: String
) extends ManagedFileStorage {

  override def toString = s"Encrypted($parent)"

  private val key = Aes
    .keyFromHex(keyHex)
    .getOrElse(
      throw new RuntimeException(
        "Not proper key. Key must be 32 bytes encoded as a hex string (00-ff)."
      )
    )

  def destroyKey() = key.destroy()

  def sharedFolder(prefix: Seq[String]): IO[Option[File]] = IO.pure(None)

  def contains(
      path: ManagedFilePath,
      retrieveSizeAndHash: Boolean
  ): IO[Option[SharedFile]] = parent.contains(path, retrieveSizeAndHash)

  def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] =
    parent.contains(path, size, hash)

  def sink(
      path: ProposedManagedFilePath
  ): Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = { (in: Stream[IO, Byte]) =>
    Stream.eval(SecureRandom.javaSecuritySecureRandom[IO]).flatMap {
      implicit secureRandom =>
        in.through(Aes.encrypt(key)).through(parent.sink(path))
    }
  }

  def stream(
      path: ManagedFilePath,
      fromOffset: Long
  ): Stream[IO, Byte] = {
    parent.stream(path, 0L).through(Aes.decrypt(key)).drop(fromOffset)
  }

  def exportFile(path: ManagedFilePath): Resource[IO, File] =
    parent.exportFile(path).flatMap { encrypted =>
      Resource.make(IO {
        val tmp = TempFile.createTempFile("")
        fs2.io.file
          .Files[IO]
          .readAll(fs2.io.file.Path.fromNioPath(encrypted.toPath))
          .through(Aes.decrypt(key))
          .through(
            fs2.io.file
              .Files[IO]
              .writeAll(fs2.io.file.Path.fromNioPath(tmp.toPath))
          )
          .compile
          .drain
          .map { _ =>
            tmp
          }
      }.flatten)(f => IO.delay(f.delete))
    }

  def uri(mp: ManagedFilePath): IO[tasks.util.Uri] =
    parent.uri(mp)

  def delete(mp: ManagedFilePath, size: Long, hash: Int) =
    parent.delete(mp, size, hash)

}
