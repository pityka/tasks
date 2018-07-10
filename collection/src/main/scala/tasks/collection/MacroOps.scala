package tasks.collection

import tasks.queue._
import tasks._
import akka.stream.scaladsl._
import scala.concurrent.Future

trait MacroOps {
  import scala.language.experimental.macros

  def repartition[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .repartitionMacro[A]

  def map[A, B](taskID: String, taskVersion: Int)(
      fun: A => B): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapMacro[A, B]

  def collect[A, B](taskID: String, taskVersion: Int)(
      fun: PartialFunction[A, B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .collectMacro[A, B]

  def scan[A, B, C](taskID: String, taskVersion: Int)(nameFun: B => String)(
      partitionSize: Long,
      prepareB: B => ComputationEnvironment => Future[C],
      fun: (C, A) => C): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .scanMacro[A, B, C]

  def mapSourceWith[A, B, C](taskID: String, taskVersion: Int)(
      nameFun: B => String)(fun: (Source[A, _], B) => ComputationEnvironment => Source[
                              C,
                              _]): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .mapSourceWithMacro[A, B, C]

  def mapFullSourceWith[A, B, C](taskID: String, taskVersion: Int)(
      nameFun: B => String)(partitionSize: Long,
                            fun: (Source[A, _], B) => ComputationEnvironment => Source[
                              C,
                              _]): TaskDefinition[(EColl[A], B), EColl[C]] =
    macro Macros
      .mapFullSourceWithMacro[A, B, C]

  def mapElemWith[A, B, C, D](taskID: String, taskVersion: Int)(
      fun1: B => ComputationEnvironment => Future[C])(
      fun2: (A, C) => D): TaskDefinition[(EColl[A], B), EColl[D]] =
    macro Macros
      .mapElemWithMacro[A, B, C, D]

  def filter[A](taskID: String, taskVersion: Int)(
      fun: A => Boolean): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .filterMacro[A]

  def mapConcat[A, B](taskID: String, taskVersion: Int)(
      fun: A => Iterable[B]): TaskDefinition[EColl[A], EColl[B]] =
    macro Macros
      .mapConcatMacro[A, B]

  def sortBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros
      .sortByMacro[A]

  def groupBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String,
      maxParallelJoins: Option[Int]): TaskDefinition[EColl[A], EColl[Seq[A]]] =
    macro Macros
      .groupByMacro[A]

  def groupBySorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[EColl[A], EColl[Seq[A]]] =
    macro Macros
      .groupBySortedMacro[A]

  def outerJoinBy[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String,
      maxParallelJoins: Option[Int]): TaskDefinition[List[EColl[A]],
                                                     EColl[Seq[Option[A]]]] =
    macro Macros.outerJoinByMacro[A]

  def outerJoinBySorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      fun: A => String): TaskDefinition[List[EColl[A]], EColl[Seq[Option[A]]]] =
    macro Macros.outerJoinBySortedMacro[A]

  def innerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String,
      maxParallelJoins: Option[Int]): TaskDefinition[(EColl[A], EColl[B]),
                                                     EColl[(A, B)]] =
    macro Macros.innerJoinBy2Macro[A, B]

  def outerJoinBy2[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String,
      maxParallelJoins: Option[Int]): TaskDefinition[(EColl[A], EColl[B]),
                                                     EColl[(Option[A],
                                                            Option[B])]] =
    macro Macros.outerJoinBy2Macro[A, B]

  def outerJoinBy2Sorted[A, B](taskID: String, taskVersion: Int)(
      partitionSize: Long,
      funA: A => String,
      funB: B => String): TaskDefinition[(EColl[A], EColl[B]),
                                         EColl[(Option[A], Option[B])]] =
    macro Macros.outerJoinBy2SortedMacro[A, B]

  def foldLeft[A, B](taskID: String, taskVersion: Int)(
      zero: B,
      fun: (B, A) => B): TaskDefinition[EColl[A], B] =
    macro Macros
      .foldLeftMacro[A, B]

  def foldLeftWith[A, B, C, D](taskID: String, taskVersion: Int)(
      zero: B,
      fun1: C => ComputationEnvironment => Future[D],
      fun2: (B, A, D) => B): TaskDefinition[(EColl[A], C), EColl[B]] =
    macro Macros
      .foldLeftWithMacro[A, B, C, D]

  def take[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[(EColl[A], Int), EColl[A]] =
    macro Macros.takeMacro[A]

  def uniqueSorted[A](taskID: String, taskVersion: Int)(
      partitionSize: Long): TaskDefinition[EColl[A], EColl[A]] =
    macro Macros.uniqueSortedMacro[A]

  def toSeq[A](taskID: String,
               taskVersion: Int): TaskDefinition[EColl[A], Seq[A]] =
    macro Macros.toSeqMacro[A]

  def reduce[A](taskID: String, taskVersion: Int)(
      fun: (A, A) => A): TaskDefinition[EColl[A], A] =
    macro Macros
      .reduceMacro[A]

  def reduceSeq[A](taskID: String, taskVersion: Int)(
      fun: Seq[A] => A): TaskDefinition[EColl[Seq[A]], EColl[A]] =
    macro Macros
      .reduceSeqMacro[A]
}
