package tasks.queue

trait Serializer[A] {
  def apply(a: A): Array[Byte]
}

trait Deserializer[A] {
  def apply(b: Array[Byte]): A
}
