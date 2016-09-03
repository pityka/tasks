package sun.net.www.protocol.s3;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.amazonaws.services.s3.AmazonS3Client;

class S3Handler extends URLStreamHandler {

  private val s3Client = new AmazonS3Client();

  def openConnection(url: URL): URLConnection =
    new URLConnection(url) {

      val bucket = url.getHost

      val key = url.getPath.drop(1)

      override def getInputStream: InputStream = {
        val s3object = s3Client.getObject(bucket, key)
        s3object.getObjectContent
      }

      override def getContentLengthLong: Long = {
        s3Client.getObjectMetadata(bucket, key).getInstanceLength
      }

      def connect = ()

      override def getHeaderField(s: String): String = s match {
        case "ETag" => s3Client.getObjectMetadata(bucket, key).getETag
        case _ => null
      }

    }
}
