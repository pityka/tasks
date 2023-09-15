package tasks.fileservice.proxy

import cats.effect._
import org.http4s._
import org.http4s.dsl.io._
import tasks.util.config.TasksConfig
import tasks.fileservice.ManagedFileStorage



object ProxyFileStorage {
  def service(storage: ManagedFileStorage) = {

   HttpRoutes.of[IO] {
    case req @ POST -> Root / "fileservice"/ "proxy" / "uri" =>
      for {
	  // Decode a User request
	  user <- req.as[ManagedFilePath]
	  // Encode a hello response
	  resp <- Ok(Hello(user.name).asJson)
    } yield (resp)
  }

  }

  def makeClient()(implicit
      config: TasksConfig,      
  ) = {
    // val url =
    //   s"akka://tasks@${address.getHostName}:${address.getPort}/user/fileservice"
   


  }
}

  // def uri(mp: ManagedFilePath): IO[Uri]

  // def stream(
  //     path: ManagedFilePath,
  //     fromOffset: Long
  // ): Stream[IO, Byte]

  // /* If size < 0 then it must not check the size and the hash
  //  *  but must return true iff the file is readable
  //  */
  // def contains(path: ManagedFilePath, size: Long, hash: Int): IO[Boolean]

  // def contains(
  //     path: ManagedFilePath,
  //     retrieveSizeAndHash: Boolean
  // ): IO[Option[SharedFile]]

  // def importFile(
  //     f: File,
  //     path: ProposedManagedFilePath
  // ): IO[(Long, Int, ManagedFilePath)]

  // def sink(
  //     path: ProposedManagedFilePath
  // ): fs2.Pipe[IO,Byte,(Long, Int, ManagedFilePath)] 

  // def exportFile(path: ManagedFilePath): Resource[IO, File]

  // def sharedFolder(prefix: Seq[String]): IO[Option[File]]

  // def delete(
  //     path: ManagedFilePath,
  //     expectedSize: Long,
  //     expectedHash: Int
  // ): IO[Boolean]

// class ProxyFileStorageClient(
//     address: String,
//     httpClient : Client[IO]
// )(implicit
//     val ec: ExecutionContext,
//     config: TasksConfig,
// ) extends ManagedFileStorage {

  

// }
