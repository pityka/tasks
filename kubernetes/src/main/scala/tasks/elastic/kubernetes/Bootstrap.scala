package tasks.elastic.kubernetes

import com.typesafe.config.Config
import tasks.defaultTaskSystem
import com.typesafe.config.ConfigFactory
import com.google.cloud.tools.jib.api.Containerizer
import tasks.util.config.TasksConfig
import io.k8s.api.core.v1.PodSpec
import io.k8s.api.core.v1.Pod
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import io.k8s.api.core.v1.Container
import io.k8s.api.core.v1.EnvVar
import io.k8s.api.core.v1.EnvVarSource
import io.k8s.api.core.v1.ObjectFieldSelector
import io.k8s.api.core.v1.ResourceRequirements
import tasks.shared.ResourceRequest
import io.k8s.apimachinery.pkg.api.resource.Quantity
import scala.util.Try
import com.goyeau.kubernetes.client.KubernetesClient
import cats.effect.IO
import tasks.TaskSystemComponents
import com.google.cloud.tools.jib.api.LogEvent
import com.google.cloud.tools.jib.api.LogEvent.Level.LIFECYCLE
import com.google.cloud.tools.jib.api.LogEvent.Level.PROGRESS
import com.google.cloud.tools.jib.api.LogEvent.Level.ERROR
import com.google.cloud.tools.jib.api.LogEvent.Level.WARN
import com.google.cloud.tools.jib.api.LogEvent.Level.DEBUG
import com.google.cloud.tools.jib.api.LogEvent.Level.INFO
import cats.effect.kernel.Resource
import tasks.deploy.HostConfiguration

object Bootstrap {

  /** Entry point with 2 way branch:
    *   - \1. config.kubernetesHostNameOrIPEnvVar env var is present then we run
    *     in kube. In that case we launch the tasksystem
    *   - The tasksystem startup will decide whether it starts in app, queue or
    *     follower mode
    *   - The user of the tasksystem resource needs to act accordingly, in
    *     particular it should IO-block forever when it is a follower
    *   - 2. MY_POD_IP not present then we run outside of kube. Creates a
    *     container and launches it which will eventually end up in 1. This
    *     process continues to trail the log of the newly created container
    *
    * @param config
    * @param containerizer
    * @return
    *   If this is an App role then whatever the app produces in an option
    *   (Some[T]) otherwise if this is a bootstrap process or worker process
    *   then returns None
    *   - if this is a bootstrap process then the IO completes when the log
    *     stream completes
    *   - if this is a worker role then the IO never completes (pod must be
    *     deleted by the main role)
    *   - if this is a master role then the IO completes when the useTs
    *     completes and the task system is closed
    */
  def entrypoint[T](
      containerizer: Containerizer,
      k8sClientResource: Resource[IO, KubernetesClient[IO]],
      mainClassName: String,
      config: Option[Config] = None,
      k8sRequestCpu: Double = 0.2,
      k8sRequestRamMB: Int = 500,
      k8sRequestEphemeralMB: Int = 0
  )(
      useTs: TaskSystemComponents => IO[T]
  ): IO[Option[T]] = {

    val tconfig = {
      val configuration = () => {
        ConfigFactory.invalidateCaches

        val loaded = (config.map { extraConf =>
          ConfigFactory.defaultOverrides
            .withFallback(extraConf)
            .withFallback(ConfigFactory.load)
        } getOrElse
          ConfigFactory.load)

        ConfigFactory.invalidateCaches

        loaded
      }
      tasks.util.config.parse(configuration)
    }

    val hostname = System.getenv(tconfig.kubernetesHostNameOrIPEnvVar)

    if (hostname != null) {

      scribe.info(
        s" ${tconfig.kubernetesHostNameOrIPEnvVar} env found ($hostname). Create task system."
      )

      val cfg0 = ConfigFactory.parseString(
        s"""
      hosts.hostname="$hostname" 
      hosts.numCPU = 0  
      tasks.worker-main-class = "$mainClassName"  
      tasks.elastic.engine = "tasks.elastic.kubernetes.K8SElasticSupport"
      """
      )

      val cfg = config.map(c => cfg0.withFallback(c)).getOrElse(cfg0)
      defaultTaskSystem(Some(cfg)).use { case (ts, hostConfig) =>
        if (hostConfig.myRoles.contains(tasks.deploy.App))
          useTs(ts)
            .map(Some(_))
            .flatTap(_ => IO { scribe.info("use TS completed.") })
        else
          IO { scribe.info("Fiber-blocking indefinitely.") } *> IO.never.as(
            None
          )
      }
    } else {
      scribe.info("No MY_POD_IP env found. Create pod of master.")
      k8sClientResource
        .flatMap { k8s =>
          val pathOfEntrypointInBootstrapContainer =
            tconfig.kubernetesImageApplicationSubPath
          val container = selfpackage.jib.containerize(
            out = addScribe(containerizer),
            mainClassNameArg = Some(mainClassName),
            pathInContainer = pathOfEntrypointInBootstrapContainer
          )

          scribe.info(
            s"Made container image with self package into ${container.getTargetImage()}"
          )

          val userCPURequest =
            math.max(k8sRequestCpu, tconfig.kubernetesCpuMin)
          val userRamRequest =
            math.max(k8sRequestRamMB, tconfig.kubernetesRamMin)

          val kubeCPURequest = userCPURequest + tconfig.kubernetesCpuExtra
          val kubeRamRequest = userRamRequest + tconfig.kubernetesRamExtra
          val podName = ("tasks-app-" + KubernetesHelpers.newName).take(47)

          val imageName = container.getTargetImage().toString

          val podSpecFromConfig: PodSpec = tconfig.kubernetesPodSpec
            .map { jsonString =>
              val either = io.circe.parser.decode[PodSpec](jsonString)
              either.left.foreach(error => scribe.error(error))
              either.toOption.get
            }
            .getOrElse(PodSpec(containers = Nil))

          val containerName = "tasks-app"
          val resource = Pod(
            metadata = Some(
              ObjectMeta(
                namespace = Some(tconfig.kubernetesNamespace),
                name = Some(podName)
              )
            ),
            apiVersion = Some("v1"),
            kind = Some("Pod"),
            spec = Some(
              podSpecFromConfig.copy(
                automountServiceAccountToken = Some(true),
                containers = List(
                  Container(
                    image = Some(imageName),
                    command = Some(
                      List(
                        "bash",
                        s"$pathOfEntrypointInBootstrapContainer/entrypoint.sh"
                      )
                    ),
                    name = containerName,
                    imagePullPolicy = Some(tconfig.kubernetesImagePullPolicy),
                    env = Some(
                      List(
                        EnvVar(
                          name = tconfig.kubernetesHostNameOrIPEnvVar,
                          valueFrom = Some(
                            EnvVarSource(
                              fieldRef = Some(
                                ObjectFieldSelector(
                                  fieldPath = "status.podIP"
                                )
                              )
                            )
                          )
                        )
                      )
                    ),
                    resources = Some(
                      ResourceRequirements(
                        requests = Some(
                          (Map(
                            "cpu" ->
                              Quantity(kubeCPURequest.toString),
                            "memory" -> new Quantity(s"${kubeRamRequest}M")
                          ) ++ (if (k8sRequestEphemeralMB > 0)
                                  Map(
                                    "ephemeral-storage" -> Quantity(
                                      s"${k8sRequestEphemeralMB}M"
                                    )
                                  )
                                else Map.empty))
                        ).filter(_.nonEmpty),
                        limits = None
                      )
                    )
                  )
                ),
                restartPolicy = Some("Never")
              )
            )
          )
          import io.circe.syntax._

          import io.circe.yaml.syntax._
          scribe.info(
            "K8s resource of master:\n" + resource.asJson.asYaml.spaces2
          )

          val t =
            k8s.pods
              .namespace(tconfig.kubernetesNamespace)
              .create(resource)

          val logStream = fs2.Stream.eval(t).flatMap { status =>
            if (status.isSuccess) {
              scribe.info(
                s"Created pod resource $podName . \n\n kubectl -n ${tconfig.kubernetesNamespace} delete pod $podName \n\nWaiting for Running state, then trailing its log.."
              )

              val phaseStream = fs2.Stream
                .eval(
                  k8s.pods
                    .namespace(tconfig.kubernetesNamespace)
                    .get(podName)
                )
                .flatMap { pod =>
                  val labels = pod.metadata.get.labels
                  val resourceVersion = pod.metadata.get.resourceVersion
                  k8s.pods
                    .namespace(tconfig.kubernetesNamespace)
                    .watch(labels.getOrElse(Map.empty), resourceVersion)
                    .map(_.map(_.`object`.status.get.phase))
                }

              val podIsRunning = phaseStream
                .takeThrough(either =>
                  either match {
                    case Left(error) =>
                      scribe.error(error)
                      false
                    case Right(None) =>
                      scribe.error("No phase in pod status")
                      false
                    case Right(Some(phase)) =>
                      phase != "Running" && phase != "Failed"
                  }
                )
                .evalTap(phase =>
                  IO { scribe.info(s"$podName in phase $phase") }
                )
                .compile
                .last
                .map(option =>
                  option.exists(_.exists(_.exists(_ == "Running")))
                )

              fs2.Stream
                .eval(podIsRunning)
                .flatMap(podIsRunning =>
                  if (podIsRunning)
                    k8s.pods
                      .namespace(tconfig.kubernetesNamespace)
                      .log(podName, Some(containerName), follow = true)
                      .flatMap(response => response.bodyText)
                  else fs2.Stream.empty
                )
            } else {
              scribe.error("Failed pod creation")
              fs2.Stream
                .fromIterator[IO](
                  List(s"Failed to create pod $status").iterator,
                  1
                )
            }
          }

          Resource.eval(
            logStream
              .evalMap(s => IO { print(s) })
              .compile
              .drain
              .flatTap(_ =>
                IO {
                  scribe.info(
                    s"Log stream completed. Will try to delete pod $podName"
                  )
                }
              )
              .flatMap(_ =>
                k8s.pods.namespace(tconfig.kubernetesNamespace).delete(podName)
              )
              .map(deletionStatus => {
                scribe.info(s"Deletion status $deletionStatus")
                None
              })
          )

        }
        .use(IO.pure(_))
    }

  }

  private def addScribe(containerizer: Containerizer) = {
    containerizer.addEventHandler(
      classOf[LogEvent],
      (logEvent: LogEvent) =>
        logEvent.getLevel() match {
          case LIFECYCLE => scribe.info(logEvent.getMessage())
          case PROGRESS  => scribe.info(logEvent.getMessage())
          case ERROR     => scribe.error(logEvent.getMessage())
          case WARN      => scribe.warn(logEvent.getMessage())
          case DEBUG     => scribe.debug(logEvent.getMessage())
          case INFO      => scribe.info(logEvent.getMessage())
        }
    )
  }

}
