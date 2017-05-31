/*
 * The MIT License
 *
 * Copyright (c) 2016 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks.collection

import tasks._
import upickle.default._
import upickle.Js
import scala.concurrent.Future

object Macros {
  import scala.reflect.macros.Context
  import scala.language.experimental.macros

  def mapMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[A => B]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
         tasks.AsyncTask[EColl[$a], EColl[$b]]("map-"+$taskID, $taskVersion) { t => implicit ctx =>

           val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$b]]("partition-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
           val fun = $fun
           val r = implicitly[tasks.queue.Deserializer[$a]]
           val w = implicitly[tasks.queue.Serializer[$b]]
            EColl.fromSource(t.source(idx)(r,ctx.components).map(x => fun(x)), (t.basename))(w,ctx.components)
          }

         releaseResources
         scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
           subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
         }).map(_.reduce(_ ++ _))
        }
    """
    r
  }

  def filterMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[A => Boolean]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
    tasks.AsyncTask[EColl[$a], EColl[$a]]("filter-"+$taskID, $taskVersion) { t => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$a]]("partition-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
        val fun = $fun
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$a]]
         EColl.fromSource(t.source(idx)(r,ctx.components).filter(x => fun(x)), (t.basename))(w,ctx.components)
      }

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask(i -> t)(CPUMemoryRequest(1,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _))
   }

    """
    r
  }

  def mapConcatMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[A => Iterable[B]]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
    tasks.AsyncTask[EColl[$a], EColl[$b]]("filter-"+$taskID, $taskVersion) { t => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$b]]("partition-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
        val fun = $fun
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$b]]
         EColl.fromSource(t.source(idx)(r,ctx.components).mapConcat(x => fun(x)), (t.basename))(w,ctx.components)
      }

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _))
   }
    """
    r
  }

  def sortByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      batchSize: cxt.Expr[Int],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[$a]]("sort-"+$taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,Int,EColl[$a]), EColl[$a]]("partition-"+$taskID, $taskVersion) { case (idx,batchSize,t) => implicit ctx =>
            val fun = $fun
            implicit val mat = ctx.components.actorMaterializer
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
            implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}
            val sortedSource = t.source(idx)(r,ctx.components).via(flatjoin_akka.sort(batchSize))
             EColl.fromSource(sortedSource, (t.basename))(w,ctx.components)
          }

        releaseResources
        scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
          subtask((i,$batchSize, t))(CPUMemoryRequest(1,resourceAllocated.memory))
        }).flatMap{ partitions =>
          if (partitions.size == 1) scala.concurrent.Future.successful(partitions.head)
          else {
            val fun = $fun
            implicit val mat = ctx.components.actorMaterializer
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
            implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}

            scala.concurrent.Future.sequence(partitions.map(_.partitions.head.file)).flatMap{ files =>
              val mergeFlow = flatjoin_akka.merge[$a]
              EColl.fromSource(akka.stream.scaladsl.Source(files.toList).via(mergeFlow),t.basename)(w,ctx.components)
            }
          }
        }
       }
    """
    r
  }

  def groupByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      parallelism: cxt.Expr[Int],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[Seq[$a]]]($taskID, $taskVersion) { t => implicit ctx =>
          val fun = $fun
          implicit val mat = ctx.components.actorMaterializer
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[Seq[$a]]]
          implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
          implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}
          val groupedSource = t.source(r,ctx.components).via(flatjoin_akka.groupByShardsInMemory($parallelism))
           EColl.fromSource(groupedSource, (t.basename))(w,ctx.components)
        }
    """
    r
  }

  def outerJoinByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      parallelism: cxt.Expr[Int],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[List[EColl[$a]], EColl[Seq[Option[$a]]]]($taskID, $taskVersion) { ts => implicit ctx =>
          val fun = $fun
          implicit val mat = ctx.components.actorMaterializer
          implicit val r = implicitly[tasks.queue.Deserializer[$a]]
          implicit val w = implicitly[tasks.queue.Serializer[Seq[Option[$a]]]]
          implicit val r2 = implicitly[tasks.queue.Deserializer[(Int,$a)]]
          implicit val w2 = implicitly[tasks.queue.Serializer[(Int,$a)]]
          implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
          implicit val fmt2 = tasks.collection.EColl.flatJoinFormat[(Int,$a)]
          implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}

          val catted = akka.stream.scaladsl.Source(ts.zipWithIndex).flatMapConcat(x => x._1.source.map(y =>x._2 -> y))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(ts.size,$parallelism))
           EColl.fromSource(joinedSource, (ts.map(_.basename).mkString(".x.")))(w,ctx.components)
        }
    """
    r
  }

  def foldLeftMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      zero: cxt.Expr[B],
      fun: cxt.Expr[(B, A) => B]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
        tasks.AsyncTask[EColl[$a], $b]($taskID, $taskVersion) { t => implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          val fun = $fun
          val z = $zero
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[$b]]
           t.source(r,ctx.components).runFold(z)(fun)
        }
    """
    r
  }

  def reduceMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[(A, A) => A]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], $a]("reduce-"+$taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,EColl[$a]), $a]("partition-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer
            val fun = $fun
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
             t.source(idx)(r,ctx.components).runReduce(fun)
          }

        releaseResources

        val fun = $fun
        scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
          subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
        }).map(_.reduce((x,y) => fun(x,y)))
       }
    """
    r
  }

}
