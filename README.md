# Distributed task execution
Library for data analysis workflows in scala.

Executes a graph of data transformations on a cluster of computers,
while maintaining data dependencies (files or objects ) between individual subtasks.

Results of individual subtasks are cached between runs. Upon restart only those subtasks are executed
whose output is not yet computed, or whose input has changed.

Automatic up and downscaling of the cluster on EC2 or via ssh.

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

# Details

## Workflow graph

The whole analysis workflow is represented as a graph in which each vertex is a transformation and
edges represent data dependencies. All data is immutable. These transformation tasks are put in a queue and scheduled for execution.

## External caching / memoization

The results of each transformation step is persisted to disk, thus checkpointing the state of the workflow and preventing repeating steps if the process is restarted.
This also provides a memoization effect during each run:
if the same transformation occurs multiple times in the graph, then it is computed only once.

## Distributed execution

Any number of worker nodes can join to a master node to take up jobs.
Worker nodes pull work from the master by presenting their available capacity.

## Resource allocations

The framework manages resource allocations in terms of CPU and memory.
These are not enforced, but serve as a hint to allow for efficient allocation of jobs among
available (possibly diverse) machines.

## Unified file handles

Tasks may have file dependencies, these files are transparently copied or streamed to the executing machine. Remote URLs are handled under the same API. Amazon S3 URL handlers are provided.

All generated files are stored in the same file pool, which can be an S3 bucket, or a folder
accessible from the master node.

File transfer between the master node and worker nodes are transparent, and shortcutted in case the worker node have direct access to the file pool (e.g. S3 or a distributed filesystem).

## Scaling of the compute cluster

Nodes are automatically started and stopped based on the state of the queue.
The library provides automatic scaling on Amazon EC2, or on a preconfigured set of nodes via SSH.
LSF is also available (open an issue if needed).

## Configuration

This library uses the Typesafe config library (https://github.com/typesafehub/config).
See the `tasks/src/main/resources/reference.conf` file for available configuration keys.

## Serialization

Serialization of user data is handled by uPickle in compile time.

## Persistence

Persistence is implemented with simple a filesystem base key-value store, or LevelDB.

## Distribution of application code

The master and worker nodes need to have the same copies of compiled class files.

The master node runs and HTTP server which exposes a prepackaged version of the application.
This package file has to be accessible from the master node.
SBT's universal plugin could be used, see the README in the example project and the build.sbt file in this project.

## Security

None, please use a private network.

# Licence

Author: Istvan Bartha

This repository is a fork of https://github.com/pityka/mybiotools/tree/master/Tasks , for which the copyright holder is `ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay` and was released under the MIT license. (That repository holds my post-doc works in the laboratory of Professor Jacques Fellay.)

Copyright of subsequent modifications belong to Istvan Bartha.

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
