# Distributed task execution
Library for data analysis workflows in scala.

Executes a graph of data transformations on a cluster of computers,
while maintaining data dependencies (files or objects ) between individual subtasks.

Results of individual subtasks are cached between runs. Upon restart only those subtasks are executed
whose output is not yet computed, or whose input has changed.

Automatic up and downscaling of the cluster on EC2, LSF or via ssh.

## Example

Task definitions:

```scala

case class BatchInput(
  file: Option[SharedFile],
  batchId: Option[Int])
    extends SimplePrerequisitive[BatchInput]

case class BatchResult(result:Int)


val batchCalc =
  Task[BatchInput, BatchResult]("batch") {

  case BatchInput(Some(file), Some(id)) =>
    implicit ctx =>

      val localFile : File = size.file

      /* Process input ..*/
      val result : Int = ???

      BatchResult(result)
}
```

Task execution:

```scala

withTaskSystem { implicit ts =>

  /* demonstrates sharing of files */
  val sharedFile: SharedFile = {
    val someFile : File = ???
    SharedFile(someFile, name = "myfile.txt")
  }

  val batchResult: Future[BatchResult] =    
        batchCalc(
          BatchInput(Some(sharedFile), Some(i))
        )(CPUMemoryRequest(1, 50))
      .? // returns a Future

  println(Await.result(batchResult, atMost = 10 minutes))

}

```

See `example` project for a complete example.

# Concepts

## Transformation graph

The whole analysis workflow is represented as a graph in which each vertex is a transformation,
edges represent dependencies. Data is immutable.

## Licence

Author: Istvan Bartha

This repository is a fork of https://github.com/pityka/mybiotools/tree/master/Tasks , for which the copyright holder is `ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay` and was released under the MIT license.

Copyright of subsequent modifications belong to the individual contributors.

```
The MIT License (MIT)

Original work (as in https://github.com/pityka/mybiotools/tree/master/Tasks):
Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay

Modified work (this repository): Copyright (c) 2016 Istvan Bartha

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
