package tasks.queue

trait SporeFunWithDependencies[A, B] {
  def call(dependencies: Seq[Spore[Any, Any]], a: A): B
}
trait SporeFun[A, B] {
  def call(a: A): B
}

trait Revivable[A, B] {
  def fqcn: String
  def dependencies: Seq[Spore[Any, Any]]
  def revive: SporeFun[A, B] = {
    try {
      val ctors = java.lang.Class
        .forName(fqcn)
        .getConstructors()

      val selectedCtor =
        ctors.find(_.getParameterCount == 0).getOrElse {
          throw new RuntimeException(
            s"Can't find constructors to revive spore. Available constructos of $fqcn : ${ctors.toList
                .map(ctor => (ctor, ctor.getParameters().toList))}"
          )
        }

      val sporefun = selectedCtor
        .newInstance()
        .asInstanceOf[SporeFunWithDependencies[A, B]]
      new SporeFun[A, B] {
        def call(a: A): B = sporefun.call(dependencies, a)
      }
    } catch {
      case e: java.lang.NoSuchMethodException =>
        throw SporeException(
          s"Available ctors: ${java.lang.Class
              .forName(fqcn)
              .getConstructors()
              .toList}",
          e
        )
    }
  }
}
