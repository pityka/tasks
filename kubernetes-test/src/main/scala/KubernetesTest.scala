/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
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

import cats.effect.unsafe.implicits.global

import tasks.jsonitersupport._
import com.typesafe.config.ConfigFactory
import cats.effect.IO
import cats.effect.IOApp
import tasks.elastic.kubernetes.Bootstrap
import cats.effect.kernel.Resource.ExitCase
import com.google.cloud.tools.jib.api.Containerizer
import com.google.cloud.tools.jib.api.DockerDaemonImage
import com.goyeau.kubernetes.client.KubernetesClient
import com.goyeau.kubernetes.client.KubeConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.concurrent.duration._
import com.google.cloud.tools.jib.api.LogEvent

// object KubernetesTestFollower extends App {
//   withTaskSystem { _ =>
//     Thread.sleep(100000)
//   }
// }

object KubernetesTest extends IOApp {

  val hostname = System.getenv("MY_POD_IP")

  val testTask = Task[String, Int]("kubernetestest", 1) {
    _ => implicit computationEnvironment =>
      scribe.info("Hello from task")
      IO(1)
  }

  // tasks.worker-main-class = "tasks.KubernetesTestSlave"
  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      hosts.hostname="$hostname"
      tasks.elastic.engine = "tasks.elastic.kubernetes.K8SElasticSupport"
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.kubernetes.image = "eclipse-temurin-unzip"
      tasks.kubernetes.image-pull-policy = "IfNotPresent"
      
      tasks.kubernetes.podSpec = {
        automountServiceAccountToken = false
        containers = []
      }
      
      """
    )
  }

  def useTs(implicit ts: TaskSystemComponents): IO[Unit] = {
    val f1 = testTask(("1"))(ResourceRequest(1, 500))

    val f2 = f1.flatMap(_ => testTask(("2"))(ResourceRequest(1, 500)))
    val f3 = testTask(("3"))(ResourceRequest(1, 500))
    val future = for {
      t1 <- f1
      t2 <- f2
      t3 <- f3
    } yield t1 + t2 + t3

    future.map { t =>
      assert(t == 3)
      if (t ==3) {
        scribe.info("SUCCESS")
      } else {
        scribe.error("FAIL")
      }
    }

  }

  def run(args: List[String]) = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    scribe.Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(minimumLevel = Some(scribe.Level.Trace))
      .replace()

    val kubernetesClient =
      KubernetesClient[IO](
        KubeConfig.standard[IO].map(_.withDefaultAuthorizationCache(5.minutes))
      )

    val containerizer = Containerizer
      .to(DockerDaemonImage.named("kubetestmaster"))

    val resultOrBlock = 
      Bootstrap.entrypoint(
        config = Some(testConfig2),
        containerizer = containerizer,
        k8sClientResource = kubernetesClient,
        mainClassName = "tasks.KubernetesTest"
      )(ts => useTs(ts))
    

    resultOrBlock.flatMap { x =>
      IO {
        println("Finished. Got:")
        println(x);
        println("Bye")
        cats.effect.ExitCode.Success
      }
    }

  }

  scribe.info("Exit main thread. ")

}
