/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
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

package tasks.elastic

import tasks.shared.monitor._
import tasks.shared._
import tasks.util._
import tasks.util.eq._
import tasks.util.config._

class SimpleDecideNewNode(codeVersion: CodeVersion)(
    implicit config: TasksConfig)
    extends DecideNewNode {

  def needNewNode(
      q: QueueStat,
      registeredNodes: Seq[CPUMemoryAvailable],
      pendingNodes: Seq[CPUMemoryAvailable]): Map[CPUMemoryRequest, Int] = {
    val resourceNeeded: List[CPUMemoryRequest] = q.queued.map(_._2).collect {
      case VersionedCPUMemoryRequest(v, request) if v === codeVersion => request
    }

    val availableResources: List[CPUMemoryAvailable] =
      (registeredNodes ++ pendingNodes).toList

    val (_, allocatedResources) =
      resourceNeeded.foldLeft((availableResources, List[CPUMemoryRequest]())) {
        case ((available, allocated), request) =>
          val (prefix, suffix) =
            available.span(x => !x.canFulfillRequest(request))
          val chosen = suffix.headOption
          chosen.foreach(x => assert(x.canFulfillRequest(request)))

          val transformed = chosen.map(_.substract(request))
          if (chosen.isDefined)
            (prefix ::: (transformed.get :: suffix.tail))
              .filterNot(_.isEmpty) -> (request :: allocated)
          else (available, allocated)
      }

    val nonAllocatedResources: Map[CPUMemoryRequest, Int] = {
      val map1 = resourceNeeded.groupBy(x => x).map(x => x._1 -> x._2.size)
      val map2 = allocatedResources.groupBy(x => x).map(x => x._1 -> x._2.size)
      (addMaps(map1, map2)(_ - _)).filter(x => { assert(x._2 >= 0); x._2 > 0 })

    }

    if (!nonAllocatedResources.isEmpty
        && (pendingNodes.size < config.maxPendingNodes)) {
      nonAllocatedResources

    } else Map()
  }

}
