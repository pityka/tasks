package tasks.queue

import com.github.plokhotnyuk.jsoniter_scala.core.JsonKeyCodec
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl.Dispatch
import tasks.queue.QueueImpl.ProxyResult
import tasks.shared.PendingJobId
import tasks.shared.RunningJobId
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node

private[tasks] object DynamoCodecs {

  implicit val pendingJobIdKeyCodec: JsonKeyCodec[PendingJobId] =
    QueueStateCodecs.pendingJobIdKeyCodec
  implicit val runningJobIdKeyCodec: JsonKeyCodec[RunningJobId] =
    QueueStateCodecs.runningJobIdKeyCodec
  implicit val scheduleTaskCodec: JsonValueCodec[ScheduleTask] =
    QueueStateCodecs.scheduleTaskCodec
  implicit val proxyCodec: JsonValueCodec[Proxy] = QueueStateCodecs.proxyCodec
  implicit val dispatchCodec: JsonValueCodec[Dispatch] =
    QueueStateCodecs.dispatchCodec
  implicit val proxyResultCodec: JsonValueCodec[ProxyResult] =
    QueueStateCodecs.proxyResultCodec
  implicit val optionalNodeCodec: JsonValueCodec[Option[Node]] =
    QueueStateCodecs.optionalNodeCodec
  implicit val nodeRegistryStateCodec: JsonValueCodec[NodeRegistryState.State] =
    QueueStateCodecs.nodeRegistryStateCodec

  def encode[A](a: A)(implicit codec: JsonValueCodec[A]): Array[Byte] =
    QueueStateCodecs.encode(a)(codec)

  def decode[A](bytes: Array[Byte])(implicit codec: JsonValueCodec[A]): A =
    QueueStateCodecs.decode(bytes)(codec)

}
