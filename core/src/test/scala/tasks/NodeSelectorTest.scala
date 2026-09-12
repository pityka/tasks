/*
 * The MIT License
 *
 * Copyright (c) 2026 Istvan Bartha
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

package tasks

import com.github.plokhotnyuk.jsoniter_scala.core.{
  readFromString,
  writeToString
}
import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import tasks.shared._

class NodeSelectorTest extends FunSuite with Matchers {

  import NodeSelector._

  test("Always matches any label set") {
    matches(Always, Set.empty) shouldBe true
    matches(Always, Set("gpu:a100", "region:us-east")) shouldBe true
  }

  test("Has requires the label to be present") {
    matches(Has("gpu:a100"), Set("gpu:a100")) shouldBe true
    matches(Has("gpu:a100"), Set("gpu:v100")) shouldBe false
    matches(Has("gpu:a100"), Set.empty) shouldBe false
  }

  test("Not inverts the inner selector — node avoidance") {
    matches(Not(Has("spot")), Set("on-demand")) shouldBe true
    matches(Not(Has("spot")), Set("spot")) shouldBe false
  }

  test("And requires every selector to match") {
    val sel = And(List(Has("region:us-east"), Has("gpu:a100")))
    matches(sel, Set("region:us-east", "gpu:a100")) shouldBe true
    matches(sel, Set("region:us-east")) shouldBe false
    matches(sel, Set.empty) shouldBe false
  }

  test("Or matches if any selector matches") {
    val sel = Or(List(Has("zone:a"), Has("zone:b")))
    matches(sel, Set("zone:a")) shouldBe true
    matches(sel, Set("zone:b")) shouldBe true
    matches(sel, Set("zone:c")) shouldBe false
  }

  test("affinity + avoidance composes via And + Not(Has)") {
    val sel = And(List(Has("region:us-east"), Not(Has("spot"))))
    matches(sel, Set("region:us-east", "on-demand")) shouldBe true
    matches(sel, Set("region:us-east", "spot")) shouldBe false
    matches(sel, Set("region:us-west")) shouldBe false
  }

  test("ResourceAvailable.canFulfillRequest honours the selector") {
    val workerA = ResourceAvailable(
      cpu = 4,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = None,
      labels = Set("region:us-east", "gpu:a100")
    )
    val workerB = ResourceAvailable(
      cpu = 4,
      memory = 1000,
      scratch = 0,
      gpu = Nil,
      image = None,
      labels = Set("region:us-west")
    )

    val baseReq =
      ResourceRequest((1, 1), 100, 0, 0, image = None, nodeSelector = None)

    workerA.canFulfillRequest(baseReq) shouldBe true
    workerB.canFulfillRequest(baseReq) shouldBe true

    val affinityReq =
      baseReq.copy(nodeSelector = Some(Has("gpu:a100")))
    workerA.canFulfillRequest(affinityReq) shouldBe true
    workerB.canFulfillRequest(affinityReq) shouldBe false

    val avoidanceReq =
      baseReq.copy(nodeSelector = Some(Not(Has("gpu:a100"))))
    workerA.canFulfillRequest(avoidanceReq) shouldBe false
    workerB.canFulfillRequest(avoidanceReq) shouldBe true
  }

  test("substract / addBack preserve labels on ResourceAvailable") {
    val worker = ResourceAvailable(
      cpu = 8,
      memory = 4000,
      scratch = 0,
      gpu = Nil,
      image = None,
      labels = Set("zone:a")
    )
    val req = ResourceRequest((2, 2), 1000, 0, 0, None, None)
    worker.substract(req).labels shouldBe Set("zone:a")
    worker.substractAll.labels shouldBe Set("zone:a")

    val alloc = ResourceAllocated(2, 1000, 0, Nil, None)
    worker.substract(alloc).labels shouldBe Set("zone:a")
    ResourceAvailable(0, 0, 0, Nil, None, Set("zone:a"))
      .addBack(alloc)
      .labels shouldBe Set("zone:a")
  }

  test("on-disk compat: old JSON without nodeSelector field decodes to None") {
    val oldJson =
      """{"cpu":[2,2],"memory":100,"scratch":0,"gpu":0,"image":null}"""
    val req = readFromString[ResourceRequest](oldJson)
    req.nodeSelector shouldBe None
    req.cpu shouldBe ((2, 2))
    req.memory shouldBe 100
  }

  test("on-disk compat: old JSON without labels field decodes to empty Set") {
    val oldJson =
      """{"cpu":4,"memory":1000,"scratch":0,"gpu":[],"image":null}"""
    val avail = readFromString[ResourceAvailable](oldJson)
    avail.labels shouldBe Set.empty
    avail.cpu shouldBe 4
  }

  test("NodeSelector codec roundtrips for composite expressions") {
    val sel: NodeSelector = And(
      List(
        Has("region:us-east"),
        Or(List(Has("zone:a"), Has("zone:b"))),
        Not(Has("spot"))
      )
    )
    val json = writeToString(sel)
    val back = readFromString[NodeSelector](json)
    back shouldBe sel
  }
}
