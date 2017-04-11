// package sun.net.www.protocol.s3;
//
// import java.io.InputStream;
// import java.net.URL;
// import java.net.URLConnection;
// import java.net.URLStreamHandler;
//
// import tasks.util.S3Helpers
// import akka.stream._
// import akka.actor._
// import com.bluelabs.s3stream._
// import com.bluelabs.akkaaws._
// import akka.stream.scaladsl._
// import scala.concurrent._
// import scala.concurrent.duration._
//
// object URLHandlerActorSystem {
//   implicit val system = ActorSystem("urlhandler")
//   implicit val mat = ActorMaterializer()
//   val s3stream = new com.bluelabs.s3stream.S3Stream(S3Helpers.credentials)
// }
//
// class S3Handler extends URLStreamHandler {
//
//   import URLHandlerActorSystem._
//   import system.dispatcher
//
//   override def finalize = system.shutdown
//
//   def openConnection(url: URL): URLConnection =
//     new URLConnection(url) {
//
//       val bucket = url.getHost
//
//       val key = url.getPath.drop(1)
//
//       override def getInputStream: InputStream =
//         s3stream
//           .download(S3Location(bucket, key))
//           .runWith(StreamConverters.asInputStream())
//
//       override def getContentLengthLong: Long = {
//         Await.result(s3stream
//                        .getMetadata(S3Location(bucket, key))
//                        .map(_.contentLength.get),
//                      atMost = 5 seconds)
//       }
//
//       def connect = ()
//
//       override def getHeaderField(s: String): String = s match {
//         case "ETag" =>
//           Await.result(
//               s3stream.getMetadata(S3Location(bucket, key)).map(_.eTag.get),
//               atMost = 5 seconds)
//         case _ => null
//       }
//
//     }
// }
