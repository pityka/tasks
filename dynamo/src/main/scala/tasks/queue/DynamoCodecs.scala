package tasks.queue

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

import tasks.elastic.NodeRegistryState
import tasks.queue.QueueImpl.Dispatch
import tasks.queue.QueueImpl.ProxyResult
import tasks.shared.PendingJobId
import tasks.shared.RunningJobId
import tasks.util.message.MessageData.ScheduleTask
import tasks.util.message.Node
import tasks.wire._

private[tasks] object DynamoCodecs {

  private val readerConfig = ReaderConfig
    .withMaxBufSize(2147483645)
    .withMaxCharBufSize(2147483645)

  implicit val pendingJobIdKeyCodec: JsonKeyCodec[PendingJobId] =
    new JsonKeyCodec[PendingJobId] {
      def decodeKey(in: JsonReader): PendingJobId =
        PendingJobId(in.readKeyAsString())
      def encodeKey(x: PendingJobId, out: JsonWriter): Unit =
        out.writeKey(x.value)
    }

  implicit val runningJobIdKeyCodec: JsonKeyCodec[RunningJobId] =
    new JsonKeyCodec[RunningJobId] {
      def decodeKey(in: JsonReader): RunningJobId =
        RunningJobId(in.readKeyAsString())
      def encodeKey(x: RunningJobId, out: JsonWriter): Unit =
        out.writeKey(x.value)
    }

  implicit val scheduleTaskCodec: JsonValueCodec[ScheduleTask] =
    JsonCodecMaker.make

  implicit val proxyCodec: JsonValueCodec[Proxy] = JsonCodecMaker.make

  implicit val dispatchCodec: JsonValueCodec[Dispatch] = JsonCodecMaker.make

  implicit val proxyResultCodec: JsonValueCodec[ProxyResult] =
    JsonCodecMaker.make

  implicit val optionalNodeCodec: JsonValueCodec[Option[Node]] =
    JsonCodecMaker.make

  implicit val nodeRegistryStateCodec: JsonValueCodec[NodeRegistryState.State] =
    JsonCodecMaker.make

  def encode[A](a: A)(implicit codec: JsonValueCodec[A]): Array[Byte] =
    writeToArray[A](a)

  def decode[A](bytes: Array[Byte])(implicit
      codec: JsonValueCodec[A]
  ): A = readFromArray[A](bytes, readerConfig)

}
