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

import tasks.util.message.QueueStat
import tasks.shared._
import tasks.util._
import tasks.util.eq._
import tasks.util.config._

private[tasks] class SimpleDecideNewNode(codeVersion: CodeVersion)(implicit
    config: TasksConfig
) extends DecideNewNode {

  def needNewNode(
      q: QueueStat,
      registeredNodes: Seq[ResourceAvailable],
      pendingNodes: Seq[ResourceAvailable]
  ): Map[ResourceRequest, Int] = {
    val resourceRequests: List[ResourceRequest] = q.queued.map(_._2).collect {
      case VersionedResourceRequest(v, request) if v === codeVersion => request
    }
    val resourcesUsedByRunningJobs: List[ResourceAllocated] =
      q.running.map(_._2).collect {
        case VersionedResourceAllocated(v, allocated) if v === codeVersion =>
          allocated
      }

    val availableResources: List[ResourceAvailable] =
      (registeredNodes ++ pendingNodes).toList

    val availableResourcesMinusRunningJobs =
      (resourcesUsedByRunningJobs).foldLeft(List.empty[ResourceAvailable]) {
        case (available, runningJob) =>
          val (prefix, suffix) =
            available.span(x => !x.canFulfillRequest(runningJob))
          val chosen = suffix.headOption
          chosen.foreach(x => assert(x.canFulfillRequest(runningJob)))

          val transformed = chosen.map(_.substract(runningJob))
          if (transformed.isDefined)
            (prefix ::: (transformed.get :: suffix.tail))
          else {
            // scribe.debug(
            //   "More resources running than available??"
            // )
            available
          }
      }
    val (_, allocatedRequests) =
      resourceRequests.foldLeft(
        (availableResourcesMinusRunningJobs, List[ResourceRequest]())
      ) { case ((available, allocated), request) =>
        val (prefix, suffix) =
          available.span(x => !x.canFulfillRequest(request))
        val chosen = suffix.headOption
        chosen.foreach(x => assert(x.canFulfillRequest(request)))

        val transformed = chosen.map(_.substract(request))
        if (chosen.isDefined)
          (prefix ::: (transformed.get :: suffix.tail)) -> (request :: allocated)
        else (available, allocated)
      }

    val nonAllocatedResources: Map[ResourceRequest, Int] = {
      val need = (resourceRequests)
        .groupBy(x => x)
        .map(x => x._1 -> x._2.size)
      val fulfilled =
        allocatedRequests.groupBy(x => x).map(x => x._1 -> x._2.size)
      // scribe.debug(
      //   s"Queued resource requests: $need. Requests allocable with current running or pending nodes: $fulfilled. Current nodes: $registeredNodes , pending: $pendingNodes"
      // )
      (addMaps(need, fulfilled)(_ - _)).filter(x => x._2 > 0)

    }

    if (
      !nonAllocatedResources.isEmpty
      && (pendingNodes.size < config.maxPendingNodes)
    ) {
      nonAllocatedResources

    } else Map()
  }

}
