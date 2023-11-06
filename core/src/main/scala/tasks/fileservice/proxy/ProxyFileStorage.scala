package tasks.fileservice.proxy

import cats.effect._
import org.http4s._
import org.http4s.dsl.io._
import tasks.SharedFile
import tasks.fileservice.ManagedFileStorage
import tasks.fileservice.ManagedFilePath
import tasks.fileservice.ProposedManagedFilePath
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

object ProxyFileStorage {
  implicit private def jsonDec[T](implicit
      t: JsonValueCodec[T]
  ): EntityDecoder[IO, T] =
    EntityDecoder
      .byteArrayDecoder[IO]
      .bimap(
        identity,
        array =>
          com.github.plokhotnyuk.jsoniter_scala.core.readFromArray(array)(t)
      )
  implicit private def jsonEnc[T](implicit
      t: JsonValueCodec[T]
  ): EntityEncoder[IO, T] =
    EntityEncoder
      .byteArrayEncoder[IO]
      .contramap(tt =>
        com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(tt)(t)
      )

  implicit private val codecOS: JsonValueCodec[Option[SharedFile]] =
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make

  implicit val managedFilePathCodec: JsonValueCodec[ManagedFilePath] =
    com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make

  private case class SinkResponse(
      size: Long,
      hash: Int,
      path: ManagedFilePath
  )
  private object SinkResponse {
    import com.github.plokhotnyuk.jsoniter_scala.macros._
    implicit val codec: JsonValueCodec[SinkResponse] = JsonCodecMaker.make
  }

  def service(storage: ManagedFileStorage) = {
    HttpRoutes.of[IO] {
      case req @ GET -> Root / "fileservice" / "proxy" / "uri" =>
        for {
          mp <- req.as[ManagedFilePath]
          uri <- storage.uri(mp)
          resp <- Ok(uri)
        } yield resp
      case req @ GET -> Root / "fileservice" / "proxy" / "stream" / LongVar(
            offset
          ) =>
        for {
          mp <- req.as[ManagedFilePath]
          stream = storage.stream(mp, offset)
          resp <- Ok(stream)
        } yield resp
      case req @ PUT -> Root / "fileservice" / "proxy" / "stream" / base64 =>
        for {
          streamThroughSink <- IO {
            val pmfp = com.github.plokhotnyuk.jsoniter_scala.core
              .readFromArray[ProposedManagedFilePath](
                java.util.Base64.getDecoder().decode(base64)
              )
            val pipe = storage.sink(pmfp)
            req.body.through(pipe)
          }
          triple <- streamThroughSink.compile.lastOrError
          resp1 = {
            val (size, hash, path) = triple
            SinkResponse(size, hash, path)
          }
          resp <- Ok(resp1)
        } yield resp
      case req @ GET -> Root / "fileservice" / "proxy" / "contains" / LongVar(
            size
          ) / IntVar(hash) =>
        for {
          mp <- req.as[ManagedFilePath]
          cont <- storage.contains(mp, size, hash)
          resp <- Ok(cont.toString)
        } yield resp
      case req @ GET -> Root / "fileservice" / "proxy" / "contains2" / IntVar(
            retrieveSizeAndHash
          ) =>
        for {
          mp <- req.as[ManagedFilePath]
          cont <- storage.contains(mp, retrieveSizeAndHash > 0)
          resp <- Ok(cont)
        } yield resp
      case req @ DELETE -> Root / "fileservice" / "proxy" / "delete" / LongVar(
            size
          ) / IntVar(hash) =>
        for {
          mp <- req.as[ManagedFilePath]
          cont <- storage.delete(mp, size, hash)
          resp <- Ok(cont.toString)
        } yield resp
    }

  }

  def makeClient(
      uri: org.http4s.Uri,
  ): Resource[IO, ManagedFileStorage] = {
    EmberClientBuilder
      .default[IO]
      .build
      .map { client =>
        new ProxyFileStorageClient(uri, client)
      }
  }

  final private class ProxyFileStorageClient private[ProxyFileStorage] (
      address: org.http4s.Uri,
      httpClient: Client[IO],
  ) extends ManagedFileStorage {


    def uri(mp: ManagedFilePath): IO[tasks.util.Uri] =
      httpClient.expect[tasks.util.Uri](
        Request[IO](
          method = GET,
          uri = address / "fileservice" / "proxy" / "uri"
        ).withEntity(mp)
      )

    def stream(
        path: ManagedFilePath,
        fromOffset: Long
    ): fs2.Stream[IO, Byte] = httpClient
      .stream(
        Request[IO](
          method = GET,
          uri = address / "fileservice" / "proxy" / "stream" / fromOffset
        ).withEntity(path)
      )
      .flatMap(_.body)

    def contains(
        path: ManagedFilePath,
        retrieveSizeAndHash: Boolean
    ): IO[Option[tasks.SharedFile]] = {
      val asInt =
        (if (retrieveSizeAndHash) 1
         else 0)
      httpClient
        .expect[Option[tasks.SharedFile]](
          Request[IO](
            method = GET,
            uri = address / "fileservice" / "proxy" / "contains2" / asInt
          ).withEntity(path)
        )
    }

    /* If size < 0 then it must not check the size and the hash
     *  but must return true iff the file is readable
     */
    def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean] =
      httpClient
        .expect[String](
          Request[IO](
            method = GET,
            uri = address / "fileservice" / "proxy" / "contains" / size / hash
          ).withEntity(path)
        )
        .flatMap(str => IO(str.toBoolean))

    def delete(
        path: ManagedFilePath,
        expectedSize: Long,
        expectedHash: Int
    ): IO[Boolean] =
      httpClient
        .expect[String](
          Request[IO](
            method = DELETE,
            uri =
              address / "fileservice" / "proxy" / "delete" / expectedSize / expectedHash
          ).withEntity(path)
        )
        .flatMap(str => IO(str.toBoolean))

    def sink(
        path: ProposedManagedFilePath
    ): fs2.Pipe[IO, Byte, (Long, Int, ManagedFilePath)] = {
      (in: fs2.Stream[IO, Byte]) =>
        fs2.Stream.eval(IO {
          val encoded = java.util.Base64.getEncoder.encodeToString(
            com.github.plokhotnyuk.jsoniter_scala.core
              .writeToArray(path)
          )
          encoded
        }.flatMap { base64 =>
          httpClient
            .expect[SinkResponse](
              Request[IO](
                method = PUT,
                uri = address / "fileservice" / "proxy" / "stream" / base64
              ).withEntity(in)
            )
            .map(response => (response.size, response.hash, response.path))
        })
    }

    def exportFile(path: ManagedFilePath): Resource[IO, java.io.File] =
      Resource.make {

        val file = tasks.util.TempFile.createTempFile("")

        val download = this
          .stream(path, 0L)
          .through(
            fs2.io.file
              .Files[IO]
              .writeAll(fs2.io.file.Path.fromNioPath(file.toPath()))
          )
          .compile
          .drain

        val verify = this.contains(path, true)

        (download *> verify).map { sharedFile =>
          if (sharedFile.isEmpty) {
            throw new RuntimeException("Proxy storage: File does not exists")
          }

          val size1: Long = sharedFile.get.byteSize

          if (size1 != (file.length: Long))
            throw new RuntimeException(
              "Proxy storage: Downloaded file length != metadata"
            )

          file
        }

      }(file => IO(file.delete))

    def sharedFolder(prefix: Seq[String]): IO[Option[java.io.File]] =
      IO.pure(None)

  }

}
