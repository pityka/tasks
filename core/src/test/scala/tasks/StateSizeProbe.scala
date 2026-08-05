package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import tasks.elastic.NodeRegistryState
import tasks.fileservice.FileServicePrefix
import tasks.queue._
import tasks.shared._
import tasks.util.message._

class StateSizeProbe extends FunSuite {

  private def gzip(bytes: Array[Byte]): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val g = new java.util.zip.GZIPOutputStream(out)
    try g.write(bytes)
    finally g.close()
    out.toByteArray
  }

  private def proxyAddress(i: Int, randomChars: Int) =
    Address(
      s"ProxyTask-TaskId(some-task,1)-$i-" +
        scala.util.Random.alphanumeric.take(randomChars).mkString
    )

  private def scheduleTask(i: Int, proxy: Address, inputBytes: Int) =
    MessageData.ScheduleTask(
      description = HashedTaskDescription(
        TaskId("some-task", 1),
        scala.util.Random.alphanumeric.take(64).mkString
      ),
      inputDeserializer = Spore[AnyRef, AnyRef]("some.pkg.Deserializer$", Nil),
      outputSerializer = Spore[AnyRef, AnyRef]("some.pkg.Serializer$", Nil),
      function = Spore[AnyRef, AnyRef]("some.pkg.Jobs$body$1", Nil),
      resource = VersionedResourceRequest(
        CodeVersion("undefined"),
        ResourceRequest((1, 1), 512, 0, 1, None)
      ),
      input = MessageData.InputData(
        Base64Data(
          java.util.Base64.getEncoder
            .encodeToString(Array.fill[Byte](inputBytes)(0))
        ),
        false
      ),
      fileServicePrefix = FileServicePrefix(Vector("some-task")),
      tryCache = true,
      priority = Priority(0),
      labels = Labels(Nil),
      lineage = TaskLineage(Nil),
      proxy = proxy,
      filePrefix = "some-task"
    )

  private def stateWith(n: Int, randomChars: Int, inputBytes: Int) = {
    val entries = (0 until n).toList.map { i =>
      val proxy = proxyAddress(i, randomChars)
      val sch = scheduleTask(i, proxy, inputBytes)
      (QueueImpl.project(sch), (sch, List(Proxy(proxy))))
    }
    QueueImpl.State(
      queuedTasks = entries.toMap,
      scheduledTasks = Map.empty,
      knownLaunchers = Map.empty,
      counters = Map.empty,
      nodes = NodeRegistryState.State.empty,
      rendezvous = Map.empty,
      completedResults = Map.empty
    )
  }

  private def report(label: String, state: QueueImpl.State): Unit = {
    val raw = SerializableQueueState.encode(state)
    val gz = gzip(raw)
    println(
      f"PROBE $label%-42s json=${raw.length}%9d gzip=${gz.length}%9d"
    )
  }

  test("probe serialized state size") {
    for (n <- List(1, 100)) {
      report(s"n=$n proxy=256 input=0", stateWith(n, 256, 0))
      report(s"n=$n proxy=22  input=0", stateWith(n, 22, 0))
      report(s"n=$n proxy=256 input=1kB", stateWith(n, 256, 1024))
      report(s"n=$n proxy=22  input=1kB", stateWith(n, 22, 1024))
    }
    val maxTasks = stateWith(1000, 256, 0)
    report("n=1000 proxy=256 input=0", maxTasks)
  }

}
