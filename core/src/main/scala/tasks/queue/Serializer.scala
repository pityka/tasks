package tasks.queue

trait Serializer[A] {
  def apply(a:A) : Array[Byte]
}

trait Deserializer[A] {
  def apply(b:Array[Byte]) : A
}

object CirceSerializer {
  import io.circe.{Encoder,Decoder}
  implicit def ser[A](implicit enc:Encoder[A]) : Serializer[A] = new Serializer[A] {
    def apply(a:A) = enc(a).noSpaces.getBytes("UTF-8")
  }
  implicit def deser[A](implicit dec:Decoder[A]) : Deserializer[A] = new Deserializer[A] {
    def apply(b:Array[Byte]) = io.circe.parser.decode[A](new String(b)).right.get
  }
}
