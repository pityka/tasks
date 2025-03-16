[![](https://github.com/pityka/tasks/workflows/CI/badge.svg)](https://github.com/pityka/tasks/actions)
[![maven](https://img.shields.io/maven-central/v/io.github.pityka/tasks-core_2.13.svg)](https://repo1.maven.org/maven2/io/github/pityka/tasks-core_2.13/)




This library provides persistent memoization around asynchronous distributed computations.

It also provides a framework to execute those computations on remote machines. For the latter it supports AWS EC2, kubernetes, or ssh.

# Example

See the `example` project and the tests.

# User guide

## Task definition
Task definitions are pure Scala functions. 
For example the following defines a task.
```scala
object Jobs {
  val increment = Task[Int, Int]("increment", 1) { case input =>
    implicit env =>
     IO(input+1)
  }
}
```

Task definitions have the following restrictions which are verified during compile time:
- Tasks can not close over their lexical scope except for stable values (val members of objects). 
- Tasks must be assigned to stable identifies (to a val member of an object).
- The input and output types must have serializers available in the implicit scope. There is support for jsoniter, circe and upickle serializers.

## Task submission
Task instances are submitted by the usual Scala function call syntax. 

The following snippet would invoke the increment task and perform the followings:
- serializes the input (in this case case 41)
- submits it to the queue,
- if the same task was already done in the past then it finds, deserializes and returns the result from peristent storage. Otherwise it continues to:
- waits for a worker to take the task from the queue
- deserializes at the worker 
- invokes the body of the task definition
- serializes the result (here 42)
- ships it to the application process which invoked the task
- deserializes the result and return to the caller
```scala
implicit val ts: TaskSystemComponents = ???
val incremented: IO[Int] = Jobs.increment(41)(ResourceRequest(cpu=1, memory=500))
```

## Configuration and lifecycle
The necessary components needed to submit a task are represented by an instance of a type of `TaskSystemComponents`. 

There are two constructor methods which can provide an instance of this type:
- `tasks.defaultTaskSystem` returns one in a `Resource[IO, (TaskSystemComponents, HostConfiguration)`. Upon closure of this resource all worker nodes are terminated.
- `tasks.withTaskSystem[T](use: TaskSystemComponents => IO[T])` takes a lambda which uses the task system.
   The `use` lambda is executed only if the process has the application role. For a pure worker process the `use` lambda is not invoked, but the returned IO fiber-blocks and executes tasks in the background. It returns the result type of the lambda in a `Right` if it running the application. If it is running a worker process without the application then it returns an ExitCode in a `Left`.

### Configuration
Configuration may be passed to both constructors (see signatures). 
For the list of configuration values see the reference.conf in the source tree.
All configuration values can be override by system properties.
The library is using sconfig (https://github.com/ekrich/sconfig) for configuration. The priority order of configuration sources is:
- system properties (`ConfigFactory.defaultOverrides`) overriding configuration keys
- the configuration values passed in the constructor methods 
- `ConfigFactory.load` - see the documentation of sconfig of how this resolve exactly. 


For example the following will invoke the above `increment` task and use a local folder for checkpointing. 

```scala
    val done :IO[Either[ExitCode, Int]]= withTaskSystem(
      Some(
        ConfigFactory.parseString(
          s"tasks.fileservice.storageURI=/data"
        )
      )
    ) { implicit ts =>
      Jobs.increment(41)(ResourceRequest(1, 500))

    }
```   
The `done` effect will complete when the invoked task (here the `increment` task) is completed.
When run it will return a `Right(42)`.

In the default configuration:
- the application process also executes tasks and there are no dedicated worker nodes
- Checkpointing happens to the local folder `./` 
- Network services are not started and bound

## Persistent memoization
Tasks are normally executed at most once. A second invocation of the same task (the same task definition with the same input value) will serve the result of the first invocation from a cache. This caching is persistent and works across restarts of the process.

The persistent storage can be a globally available object storage (S3), a globally available file system (NFS mounted across all nodes) or a local file system on one of the nodes.
In the first two cases (global storage) the caching works fully distributed without a single point of failure or bottleneck.
In the case of a local storage the application process acts as a single point of failure.

The `tasks.fileservice.storageURI` configuration key must be set either to an absolute path or an S3 uri.
In the latter case the classpath must contain the `tasks-s3` and an S3 client must be passed to the tasks system constructor:

```scala
withTaskSystem(
  s= "tasks.fileservice.storageURI=s3://my-bucket/prefix",
  s3Client = tasks.fileservice.s3.S3.makeS3ClientResource("my-aws-region"),
  elasticSupport = Resource.pure(None)
)(system => ???)
```


## Worker and application roles
The application role:
- runs the business logic of the application and invokes (submits) task definitions
- it also runs an ephemeral in memory queue which organizes the submitted work across workers
- if configured so it runs a cache backed by a local folder
- if configured so it manages the lifecycle of worker processes

The worker role:
- polls the queue for jobs
- executes jobs
- the lifecycle (i.e. start and stop) of worker processes may be managed by the application process, or externally.

The worker and application roles may be present in the same process, i.e. an application process can act as a worker and consume its own queue. 

## Ephemeral queue
The queue is ephemeral. 
This library provides no solution for any kind of persisted list of submitted tasks.

The queue provides some active features:
- it monitors whether a worker nodes are alive and if not it requeues the task execution. In case of network partitions a task may be executed more than once. However tasks are pure functions so this should not pose semantic differences.
- in case of failures, failed tasks may be requeued a limited number of times

## Management of worker process life cycle
Worker processes can be spawned and stopped externally, or there is support for a simple demand based scheduling of them via ssh, kubernetes and EC2.

## File abstraction
Opaque binary blobs of data (files) in the input and output types are managed by the `tasks.SharedFile` type.
The library provides serializes, constructors (from local file, byte stream and external url) and access methods (into local file, byte stream, byte array, string) for this type. 

The application must ensure that the name of SharedFile instances are unique. 

```scala
  val task2 = Task[SharedFile, SharedFile]("task-with-file", 1) {
    inputFile => implicit env =>
      // access methods:
      inputFile.stream // fs2 byte stream
      inputFile.file // local temp file in Resource
      input.utf8 // contents in String
      input.bytes // contents in ByteVector

      // consructors:
      // constructors must provide name and data
      // SharedFile.apply overloads from fs2 stream, local file, byte array
      // SharedFile.sink fs2 Sink

      for {        
        sf2 <- SharedFile(
          fs2.Stream.chunk(fs2.Chunk.array("abcd".getBytes("UTF-8"))),
          "filename"
        )        
      } yield sf2

  }
```

## Resource allocation
Task submission can request cpu, memory, scratch and gpu resources. 
Worker roles are configured with available resources of the same types. 

Resource allocations are enforced, i.e. a task may use more resources than asked for and the library will not do anything about this.

## Horizontal scaling
Worker processes are always attached to a single master process. 
A single master process may distribute work to multiple worker processes. 

Multiple identical master processes can run aside each other if either:
- the persistent storage is globally accessible (object storage or a file system mounted on each node). If it is a network filesystem, it must provide atomic rename.
- or the persistent caching is disabled.

## Worker only process
Worker processes may be launched manually or managed by application.

Manually managed worker nodes may be launched by the `defaultTaskSystem` or `withTaskSystem` constructors by passing in the following configuration values (or setting system properties):
- `hosts.master` configuration value must be defined and must be an ip:part or hostname:port pair separted by colon.
- `hosts.app=false` must be defined and set to false
Worker and application processes must have the same classpath. If you launch workers manually you have to ensure this.


# Licence

Author: Istvan Bartha

This repository is a fork of https://github.com/pityka/mybiotools/tree/master/Tasks , for which the copyright holder is `ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland, Group Fellay` and was released under the MIT license. 

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
