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

import akka.actor._
import java.io.File
import tasks.util.Uri

sealed trait FileServiceMessage

@SerialVersionUID(1L)
case class GetListOfFilesInStorage(regexp: String) extends FileServiceMessage

@SerialVersionUID(1L)
case class NewFile(f: File, p: ProposedManagedFilePath, ephemeralFile: Boolean)
    extends FileServiceMessage

@SerialVersionUID(1L)
case class NewSource(p: ProposedManagedFilePath) extends FileServiceMessage

@SerialVersionUID(1L)
case class GetPaths(p: ManagedFilePath, size: Long, hash: Int)
    extends FileServiceMessage

@SerialVersionUID(1L)
case class KnownPaths(paths: List[File]) extends FileServiceMessage

@SerialVersionUID(1L)
case class TransferToMe(actor: ActorRef) extends FileServiceMessage

@SerialVersionUID(1L)
case class TransferFileToUser(actor: ActorRef, sf: ManagedFilePath)

@SerialVersionUID(1L)
case object WaitingForSharedFile extends FileServiceMessage

@SerialVersionUID(1L)
case object WaitingForPath extends FileServiceMessage

@SerialVersionUID(1L)
case class FileNotFound(e: Throwable) extends FileServiceMessage

@SerialVersionUID(1L)
case class Uploaded(length: Long,
                    hash: Int,
                    file: Option[File],
                    p: ManagedFilePath)
    extends FileServiceMessage

@SerialVersionUID(1L)
case class CouldNotUpload(p: ProposedManagedFilePath)
    extends FileServiceMessage

@SerialVersionUID(1L)
case class IsInStorageAnswer(value: Boolean) extends FileServiceMessage

@SerialVersionUID(1L)
case class ErrorWhileAccessingStore(e: Throwable) extends FileServiceMessage

@SerialVersionUID(1L)
case class NewRemote(uri: Uri) extends FileServiceMessage

@SerialVersionUID(1L)
case class IsAccessible(sf: ManagedFilePath, size: Long, hash: Int)
    extends FileServiceMessage

@SerialVersionUID(1L)
case class GetUri(sf: ManagedFilePath) extends FileServiceMessage
