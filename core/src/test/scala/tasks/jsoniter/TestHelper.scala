// /*
//  * The MIT License
//  *
//  * Copyright (c) 2018 Istvan Bartha
//  *
//  * Permission is hereby granted, free of charge, to any person obtaining
//  * a copy of this software and associated documentation files (the "Software"),
//  * to deal in the Software without restriction, including without limitation
//  * the rights to use, copy, modify, merge, publish, distribute, sublicense,
//  * and/or sell copies of the Software, and to permit persons to whom the Software
//  * is furnished to do so, subject to the following conditions:
//  *
//  * The above copyright notice and this permission notice shall be included in all
//  * copies or substantial portions of the Software.
//  *
//  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  * SOFTWARE.
//  */

// package tasks

// import scala.concurrent._
// import scala.concurrent.duration._

// import com.github.plokhotnyuk.jsoniter_scala.core._
// import com.github.plokhotnyuk.jsoniter_scala.macros._

// import com.typesafe.config.ConfigFactory

// trait TestHelpers {

//   def await[T](f: Future[T]) = Await.result(f, atMost = 60 seconds)

//   case class Input(i: Int)
//   object Input {
//     implicit val codec: JsonValueCodec[Input] =
//       JsonCodecMaker.make[Input]
//   }

//   case class InputSF(files1: List[SharedFile]) extends WithSharedFiles(files1)
//   object InputSF {
//     implicit val codec: JsonValueCodec[InputSF] =
//       JsonCodecMaker.make[InputSF]
//   }

//   def testConfig = {
//     val tmp = tasks.util.TempFile.createTempFile(".temp")
//     tmp.delete
//     ConfigFactory.parseString(
//       s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
//       hosts.numCPU=4
//       akka.loglevel=OFF
//       """
//     )
//   }

//   implicit val codec: JsonValueCodec[Int] =
//     JsonCodecMaker.make[Int]

// }
