package tasks.elastic.ec2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.amazonaws.ec2._
import tasks.shared.{NodeSelector, ResourceRequest}

class EC2CreateNodeTest extends AnyFunSuite with Matchers {

  private def typeInfo(
      vcpu: Int,
      memMib: Long,
      gpuCount: Int = 0,
      scratchGb: Long = 0
  ): InstanceTypeInfo =
    InstanceTypeInfo(
      vCpuInfo = Some(VCpuInfo(defaultVCpus = Some(VCpuCount(vcpu)))),
      memoryInfo = Some(MemoryInfo(sizeInMiB = Some(MemorySize(memMib)))),
      gpuInfo =
        if (gpuCount == 0) None
        else
          Some(
            GpuInfo(
              gpus = Some(
                List(
                  GpuDeviceInfo(count = Some(GpuDeviceCount(gpuCount)))
                )
              )
            )
          ),
      instanceStorageInfo =
        if (scratchGb == 0) None
        else Some(InstanceStorageInfo(totalSizeInGB = Some(DiskSize(scratchGb))))
    )

  private def req(
      cpu: Int,
      mem: Int,
      gpu: Int = 0,
      scratch: Int = 0,
      image: Option[String] = None,
      selector: Option[NodeSelector] = None
  ): ResourceRequest =
    ResourceRequest(
      cpu = (cpu, cpu),
      memory = mem,
      scratch = scratch,
      gpu = gpu,
      image = image,
      nodeSelector = selector
    )

  test("fits accepts an instance meeting the request") {
    EC2CreateNode.fits(typeInfo(4, 8000), req(2, 4000)) shouldBe true
  }

  test("fits rejects when CPU is too low") {
    EC2CreateNode.fits(typeInfo(2, 8000), req(4, 4000)) shouldBe false
  }

  test("fits rejects when memory is too low") {
    EC2CreateNode.fits(typeInfo(4, 4000), req(2, 8000)) shouldBe false
  }

  test("fits rejects when GPU count is too low") {
    EC2CreateNode.fits(
      typeInfo(4, 8000, gpuCount = 1),
      req(4, 8000, gpu = 4)
    ) shouldBe false
  }

  test("fits accepts when GPU count meets the request") {
    EC2CreateNode.fits(
      typeInfo(8, 16000, gpuCount = 4),
      req(4, 8000, gpu = 4)
    ) shouldBe true
  }

  test("fits rejects an instance without instance store when scratch is asked") {
    EC2CreateNode.fits(
      typeInfo(8, 16000),
      req(2, 4000, scratch = 1)
    ) shouldBe false
  }

  test("fits rejects when the instance store is smaller than the request") {
    EC2CreateNode.fits(
      typeInfo(8, 16000, scratchGb = 100),
      req(2, 4000, scratch = 200000)
    ) shouldBe false
  }

  test("fits accepts when the instance store covers the request") {
    EC2CreateNode.fits(
      typeInfo(8, 16000, scratchGb = 900),
      req(2, 4000, scratch = 200000)
    ) shouldBe true
  }

  test("fits and resourceAvailable agree on scratch") {
    val info = typeInfo(8, 16000, scratchGb = 474)
    val scratch = EC2CreateNode.resourceAvailable(info, None).scratch
    EC2CreateNode.fits(info, req(2, 4000, scratch = scratch)) shouldBe true
    EC2CreateNode.fits(info, req(2, 4000, scratch = scratch + 1)) shouldBe false
  }

  test("scratch is converted from decimal GB to MiB") {
    EC2CreateNode.scratchMiB(typeInfo(2, 8000, scratchGb = 1)) shouldBe 953
    EC2CreateNode.scratchMiB(typeInfo(2, 8000, scratchGb = 900)) shouldBe 858306
  }

  test("resourceAvailable exposes the actual sizing") {
    val a = EC2CreateNode
      .resourceAvailable(typeInfo(8, 32000, gpuCount = 2, scratchGb = 900), None)
    a.cpu shouldBe 8
    a.memory shouldBe 32000
    a.gpu should have size 2
    a.scratch shouldBe 858306
  }

  test("resourceAvailable threads the image through") {
    val a = EC2CreateNode.resourceAvailable(
      typeInfo(2, 8000),
      Some("registry.example/img:tag")
    )
    a.image shouldBe Some("registry.example/img:tag")
  }

  test("resourceAvailable reports zero scratch for EBS-only types") {
    val a = EC2CreateNode.resourceAvailable(typeInfo(2, 8000), None)
    a.scratch shouldBe 0
  }

  test("labelsFromSelector extracts Has-labels for user-data broadcast") {
    val sel: NodeSelector = NodeSelector.And(
      List(
        NodeSelector.Has("region:us-east"),
        NodeSelector.Or(
          List(NodeSelector.Has("gpu:a100"), NodeSelector.Has("gpu:h100"))
        ),
        NodeSelector.Not(NodeSelector.Has("noisy:true"))
      )
    )
    EC2CreateNode.labelsFromSelector(sel) shouldBe Set(
      "region:us-east",
      "gpu:a100",
      "gpu:h100"
    )
  }

  test("gzipBase64 round-trips a payload via gunzip") {
    val original = "hello, world"
    val b64 = EC2CreateNode.gzipBase64(original)
    val bytes = java.util.Base64.getDecoder.decode(b64)
    val gzin = new java.util.zip.GZIPInputStream(
      new java.io.ByteArrayInputStream(bytes)
    )
    val decoded =
      new String(gzin.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    gzin.close()
    decoded shouldBe original
  }
}
