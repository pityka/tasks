package tasks

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.github.plokhotnyuk.jsoniter_scala.core._

import tasks.shared._
import tasks.queue.{TaskId, HashedTaskDescription, Spore}
import tasks.queue.QueueStateCodecs
import tasks.util.Uri
import tasks.util.message.{Node, LauncherName}
import tasks.fileservice.{
  SharedFile,
  ManagedFilePath,
  FilePath,
  History,
  HistoryContext,
  HistoryContextImpl,
  NoHistory,
  FileServicePrefix,
  ProposedManagedFilePath
}

class WireGoldenTest extends AnyFunSuite with Matchers {

  def wire[T](x: T)(implicit codec: JsonValueCodec[T]): String = writeToString(
    x
  )

  test("RunningJobId wire format is pinned") {
    wire(RunningJobId("rj-1")) shouldBe """{"value":"rj-1"}"""
  }

  test("PendingJobId wire format is pinned") {
    wire(PendingJobId("pj-1")) shouldBe """{"value":"pj-1"}"""
  }

  test("Labels wire format is pinned") {
    wire(Labels(List("a" -> "1", "b" -> "2"))) shouldBe
      """{"values":[["a","1"],["b","2"]]}"""
  }

  test("Labels empty wire format is pinned") {
    wire(Labels.empty) shouldBe "{}"
  }

  test("Replication minimum wire format is pinned") {
    wire(Replication(1)) shouldBe """{"maximum":1}"""
  }

  test("Replication with placement wire format is pinned") {
    wire(Replication(3, "zone")) shouldBe
      """{"maximum":3,"placementAttribute":"zone"}"""
  }

  test("ResourceRequest minimal wire format is pinned") {
    wire(ResourceRequest(1, 100, 0, 0)) shouldBe
      """{"cpu":[1,1],"memory":100,"scratch":0,"gpu":0}"""
  }

  test("ResourceRequest with image wire format is pinned") {
    wire(ResourceRequest((2, 4), 200, 10, 1, Some("img:tag"))) shouldBe
      """{"cpu":[2,4],"memory":200,"scratch":10,"gpu":1,"image":"img:tag"}"""
  }

  test("ResourceAllocated wire format is pinned") {
    wire(ResourceAllocated(2, 500, 10, List(0, 1), Some("img"))) shouldBe
      """{"cpu":2,"memory":500,"scratch":10,"gpu":[0,1],"image":"img"}"""
  }

  test("ResourceAvailable wire format is pinned") {
    wire(
      ResourceAvailable(4, 1000, 20, List(0, 1), Some("img"), Set("l1", "l2"))
    ) shouldBe
      """{"cpu":4,"memory":1000,"scratch":20,"gpu":[0,1],"image":"img","labels":["l1","l2"]}"""
  }

  test("NodeSelector Always wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](Always) shouldBe """{"type":"Always"}"""
  }

  test("NodeSelector Has wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](Has("zone:a")) shouldBe
      """{"type":"Has","label":"zone:a"}"""
  }

  test("NodeSelector Not(Has) wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](Not(Has("gpu"))) shouldBe
      """{"type":"Not","selector":{"type":"Has","label":"gpu"}}"""
  }

  test("NodeSelector And wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](And(List(Has("a"), Has("b")))) shouldBe
      """{"type":"And","selectors":[{"type":"Has","label":"a"},{"type":"Has","label":"b"}]}"""
  }

  test("NodeSelector Or wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](Or(List(Has("a"), Has("b")))) shouldBe
      """{"type":"Or","selectors":[{"type":"Has","label":"a"},{"type":"Has","label":"b"}]}"""
  }

  test("TaskId wire format is pinned") {
    wire(TaskId("my-task", 3)) shouldBe
      """{"id":"my-task","version":3}"""
  }

  test("HashedTaskDescription wire format is pinned") {
    wire(HashedTaskDescription(TaskId("t", 1), "hash-xyz")) shouldBe
      """{"taskId":{"id":"t","version":1},"dataHash":"hash-xyz"}"""
  }

  test("Uri wire format is pinned") {
    wire(Uri(scheme = "http", hostname = "h", port = 80, path = "/x")) shouldBe
      """{"uri":"http://h:80/x"}"""
  }

  test("FilePath ManagedFilePath wire format is pinned") {
    wire[FilePath](ManagedFilePath(Vector("a", "b", "c"))) shouldBe
      """{"type":"ManagedFilePath","pathElements":["a","b","c"]}"""
  }

  test("SharedFile wire format is pinned") {
    wire(SharedFile(ManagedFilePath(Vector("f.txt")), 100L, 42)) shouldBe
      """{"path":{"type":"ManagedFilePath","pathElements":["f.txt"]},"byteSize":100,"hash":42}"""
  }

  test("Spore wire format is pinned") {
    wire(Spore[Int, String]("fqcn.of.class", Seq.empty)) shouldBe
      """{"fqcn":"fqcn.of.class"}"""
  }

  test("VersionedResourceRequest wire format is pinned") {
    wire(
      VersionedResourceRequest(CodeVersion("v1"), ResourceRequest(1, 100, 0, 0))
    ) shouldBe
      """{"codeVersion":{"type":"CodeVersionImpl","s":"v1"},"cpuMemoryRequest":{"cpu":[1,1],"memory":100,"scratch":0,"gpu":0}}"""
  }

  test("VersionedResourceAllocated wire format is pinned") {
    wire(
      VersionedResourceAllocated(
        CodeVersion("v1"),
        ResourceAllocated(1, 100, 0, Nil, None)
      )
    ) shouldBe
      """{"codeVersion":{"type":"CodeVersionImpl","s":"v1"},"cpuMemoryAllocated":{"cpu":1,"memory":100,"scratch":0}}"""
  }

  test("VersionedResourceAvailable wire format is pinned") {
    wire(
      VersionedResourceAvailable(
        CodeVersion("v1"),
        ResourceAvailable(1, 100, 0, Nil, None, Set.empty)
      )
    ) shouldBe
      """{"codeVersion":{"type":"CodeVersionImpl","s":"v1"},"cpuMemoryAvailable":{"cpu":1,"memory":100,"scratch":0}}"""
  }

  test("LogRecord wire format is pinned") {
    wire(
      LogRecord("hello", java.time.Instant.ofEpochMilli(1700000000000L))
    ) shouldBe
      """{"data":"hello","timestamp":"2023-11-14T22:13:20Z"}"""
  }

  test("FilePath RemoteFilePath wire format is pinned") {
    wire[FilePath](
      tasks.fileservice.RemoteFilePath(Uri("http", "h", 80, "/f"))
    ) shouldBe
      """{"type":"RemoteFilePath","uri":{"uri":"http://h:80/f"}}"""
  }

  test("ResourceRequest with nodeSelector wire format is pinned") {
    wire(
      ResourceRequest(
        (1, 1),
        100,
        0,
        0,
        None,
        Some(NodeSelector.Has("zone:a"))
      )
    ) shouldBe
      """{"cpu":[1,1],"memory":100,"scratch":0,"gpu":0,"nodeSelector":{"type":"Has","label":"zone:a"}}"""
  }

  test("ResourceRequest with replication wire format is pinned") {
    wire(
      ResourceRequest(
        (1, 1),
        100,
        0,
        0,
        None,
        None,
        Some(Replication(3))
      )
    ) shouldBe
      """{"cpu":[1,1],"memory":100,"scratch":0,"gpu":0,"replication":{"maximum":3}}"""
  }

  test("Replication with maximum and placement wire format is pinned") {
    wire(Replication(5, "rack")) shouldBe
      """{"maximum":5,"placementAttribute":"rack"}"""
  }

  test("Spore with dependencies wire format is pinned") {
    wire(
      Spore[Int, String](
        "parent.fqcn",
        Seq(
          Spore[Any, Any]("dep1.fqcn", Seq.empty),
          Spore[Any, Any]("dep2.fqcn", Seq.empty)
        )
      )
    ) shouldBe
      """{"fqcn":"parent.fqcn","deps":[{"fqcn":"dep1.fqcn"},{"fqcn":"dep2.fqcn"}]}"""
  }

  test("NodeSelector And of mixed children wire format is pinned") {
    import NodeSelector._
    wire[NodeSelector](
      And(List(Not(Has("x")), Or(List(Has("a"), Has("b")))))
    ) shouldBe
      """{"type":"And","selectors":[{"type":"Not","selector":{"type":"Has","label":"x"}},{"type":"Or","selectors":[{"type":"Has","label":"a"},{"type":"Has","label":"b"}]}]}"""
  }

  test("HistoryContextImpl wire format is pinned") {
    wire[HistoryContext](
      HistoryContextImpl(
        History.TaskVersion("t", 1),
        "v1",
        Some("trace-id")
      )
    ) shouldBe
      """{"type":"HistoryContextImpl","task":{"taskID":"t","taskVersion":1},"codeVersion":"v1","traceId":"trace-id"}"""
  }

  test("NoHistory wire format is pinned") {
    wire[HistoryContext](NoHistory) shouldBe """{"type":"NoHistory"}"""
  }

  test("History wire format is pinned") {
    wire(
      History(
        SharedFile(ManagedFilePath(Vector("x")), 10L, 1),
        Some(HistoryContextImpl(History.TaskVersion("t", 1), "v1", None))
      )
    ) shouldBe
      """{"self":{"path":{"type":"ManagedFilePath","pathElements":["x"]},"byteSize":10,"hash":1},"context":{"type":"HistoryContextImpl","task":{"taskID":"t","taskVersion":1},"codeVersion":"v1"}}"""
  }

  test("FileServicePrefix wire format is pinned") {
    wire(FileServicePrefix(Vector("a", "b"))) shouldBe
      """{"list":["a","b"]}"""
  }

  test("ProposedManagedFilePath wire format is pinned") {
    wire(ProposedManagedFilePath(Vector("x", "y"))) shouldBe
      """{"list":["x","y"]}"""
  }

  test("Node wire format is pinned") {
    wire[Option[Node]](
      Some(
        Node(
          RunningJobId("rj"),
          ResourceAvailable(1, 100, 0, Nil, None, Set.empty),
          LauncherName("L")
        )
      )
    )(QueueStateCodecs.optionalNodeCodec) shouldBe
      """{"name":{"value":"rj"},"size":{"cpu":1,"memory":100,"scratch":0},"launcherActor":{"name":"L"}}"""
  }

}
