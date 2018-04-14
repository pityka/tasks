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

object Macros {
  import scala.reflect.macros.blackbox.Context

  def repartitionMacro[A: cxt.WeakTypeTag](cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int])(partitionSize: cxt.Expr[Long]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
         tasks.AsyncTask[EColl[$a], EColl[$a]]($taskID, $taskVersion) { t => implicit ctx =>

            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            EColl.fromSource(t.source(r,ctx.components),t.basename+"."+$taskID, $partitionSize)(w,ctx.components)
        }
    """
    r
  }

  def mapMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[A => B]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
         tasks.AsyncTask[EColl[$a], EColl[$b]]($taskID, $taskVersion) { t => implicit ctx =>

           val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$b]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
           val fun = $fun
           log.info($taskID+"-"+idx)
           val r = implicitly[tasks.queue.Deserializer[$a]]
           val w = implicitly[tasks.queue.Serializer[$b]]
            EColl.fromSourceAsPartition(t.source(idx)(r,ctx.components).map(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
          }

         releaseResources
         scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
           subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
         }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID))
        }
    """
    r
  }

  def collectMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[PartialFunction[A, B]]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
         tasks.AsyncTask[EColl[$a], EColl[$b]]($taskID, $taskVersion) { t => implicit ctx =>

           val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$b]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
           val fun = $fun
           log.info($taskID+"-"+idx)
           val r = implicitly[tasks.queue.Deserializer[$a]]
           val w = implicitly[tasks.queue.Serializer[$b]]
            EColl.fromSourceAsPartition(t.source(idx)(r,ctx.components).collect(fun), t.basename+"."+$taskID, idx)(w,ctx.components)
          }

         releaseResources
         scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
           subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
         }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID))
        }
    """
    r
  }

  def reduceSeqMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[Seq[A] => A]
  ) = mapMacro[Seq[A], A](cxt)(taskID, taskVersion)(fun)

  def filterMacro[A: cxt.WeakTypeTag](cxt: Context)(taskID: cxt.Expr[String],
                                                    taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[A => Boolean]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
    tasks.AsyncTask[EColl[$a], EColl[$a]]($taskID, $taskVersion) { t => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$a]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
        val fun = $fun
        log.info($taskID+"-"+idx)
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$a]]
         EColl.fromSourceAsPartition(t.source(idx)(r,ctx.components).filter(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
      }

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask(i -> t)(CPUMemoryRequest(1,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID))
   }

    """
    r
  }

  def mapSourceWithMacro[A: cxt.WeakTypeTag,
                         B: cxt.WeakTypeTag,
                         C: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[
        (akka.stream.scaladsl.Source[A, _],
         B) => tasks.queue.ComputationEnvironment => akka.stream.scaladsl.Source[
          C,
          _]]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]
    val c = weakTypeOf[C]

    val r = q"""
    tasks.AsyncTask[(EColl[$a],$b), EColl[$c]]($taskID, $taskVersion) { case (t,b) => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a], $b), EColl[$c]]("sub-"+$taskID, $taskVersion) { case (idx,t,b) => implicit ctx =>
        val fun = $fun
        log.info($taskID+"-"+idx)
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$c]]
         EColl.fromSourceAsPartition(fun(t.source(idx)(r,ctx.components),b)(ctx), t.basename+"."+$taskID, idx)(w,ctx.components)
      }

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask((i,t,b))(CPUMemoryRequest(1,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID))
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
    tasks.AsyncTask[EColl[$a], EColl[$b]]($taskID, $taskVersion) { t => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$b]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
        val fun = $fun
        log.info($taskID+"-"+idx)
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$b]]
         EColl.fromSourceAsPartition(t.source(idx)(r,ctx.components).mapConcat(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
      }

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID))
   }
    """
    r
  }

  def sortByMacro[A: cxt.WeakTypeTag](cxt: Context)(taskID: cxt.Expr[String],
                                                    taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      batchSize: cxt.Expr[Int],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[$a]]($taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,Int,EColl[$a]), EColl[$a]]("sub-"+$taskID, $taskVersion) { case (idx,batchSize,t) => implicit ctx =>
            val fun = $fun
            log.info($taskID+"-"+idx)
            implicit val mat = ctx.components.actorMaterializer
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
            implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}
            val sortedSource = t.source(idx)(r,ctx.components).via(flatjoin_akka.sort(batchSize))
             EColl.fromSourceAsPartition(sortedSource, t.basename+"."+$taskID, idx)(w,ctx.components)
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
              EColl.fromSource(akka.stream.scaladsl.Source(files.toList).via(mergeFlow),t.basename+"."+$taskID,$partitionSize)(w,ctx.components)
            }
          }
        }
       }
    """
    r
  }

  def groupByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[Seq[$a]]]($taskID, $taskVersion) { t => implicit ctx =>
          val fun = $fun
          log.info($taskID)
          implicit val mat = ctx.components.actorMaterializer
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[Seq[$a]]]
          implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
          implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}
          val groupedSource = t.source(r,ctx.components).via(flatjoin_akka.groupByShardsInMemory(resourceAllocated.cpu))
           EColl.fromSource(groupedSource, (t.basename+"."+$taskID),$partitionSize)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[List[EColl[$a]], EColl[Seq[Option[$a]]]]($taskID, $taskVersion) { ts => implicit ctx =>
          val fun = $fun
          log.info($taskID)
          implicit val mat = ctx.components.actorMaterializer
          implicit val r = implicitly[tasks.queue.Deserializer[$a]]
          implicit val w = implicitly[tasks.queue.Serializer[Seq[Option[$a]]]]
          implicit val r2 = implicitly[tasks.queue.Deserializer[(Int,$a)]]
          implicit val w2 = implicitly[tasks.queue.Serializer[(Int,$a)]]
          implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
          implicit val fmt2 = tasks.collection.EColl.flatJoinFormat[(Int,$a)]
          implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}

          val catted = akka.stream.scaladsl.Source(ts.zipWithIndex).flatMapConcat(x => x._1.source.map(y =>x._2 -> y))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(ts.size,resourceAllocated.cpu))
          EColl.fromSource(joinedSource, ($taskID+"."+ts.map(_.basename).mkString(".x.")),$partitionSize)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinBy2Macro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      funA: cxt.Expr[A => String],
      funB: cxt.Expr[B => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
        tasks.AsyncTask[(EColl[$a],EColl[$b]), EColl[(Option[$a],Option[$b])]]($taskID, $taskVersion) { case (as,bs) => implicit ctx =>
          val funA = $funA
          val funB = $funB
          log.info($taskID)
          implicit val mat = ctx.components.actorMaterializer
          implicit val rA = implicitly[tasks.queue.Deserializer[$a]]
          implicit val rB = implicitly[tasks.queue.Deserializer[$b]]
          
          implicit val w = implicitly[tasks.queue.Serializer[(Option[$a],Option[$b])]]

          implicit val r2 = implicitly[tasks.queue.Deserializer[(Int,Either[$a,$b])]]
          implicit val w2 = implicitly[tasks.queue.Serializer[(Int,Either[$a,$b])]]

          implicit val fmt = tasks.collection.EColl.flatJoinFormat[Either[$a,$b]]
          implicit val fmt2 = tasks.collection.EColl.flatJoinFormat[(Int,Either[$a,$b])]

          
          implicit val sk = new flatjoin.StringKey[Either[$a,$b]] { def key(t:Either[$a,$b]) = t match {
            case Left(a) => funA(a)
            case Right(b) => funB(b)
          }}
       
          val catted : akka.stream.scaladsl.Source[(Int,Either[$a,$b]),_] = as.source.map(a => 0 -> Left[$a,$b](a)) ++ bs.source.map(b => 1 -> Right[$a,$b](b))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(2,resourceAllocated.cpu))
          val unzippedSource = joinedSource.map{ seq =>
            val a = seq(0)
            val b = seq(1)
            (a.map(_.left.get),b.map(_.right.get))
          }
           EColl.fromSource(unzippedSource, $taskID+"."+as.basename+".x."+bs.basename, $partitionSize)(w,ctx.components)
        }
    """
    r
  }

  def innerJoinBy2Macro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      funA: cxt.Expr[A => String],
      funB: cxt.Expr[B => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]

    val r = q"""
        tasks.AsyncTask[(EColl[$a],EColl[$b]), EColl[($a,$b)]]($taskID, $taskVersion) { case (as,bs) => implicit ctx =>
          val funA = $funA
          val funB = $funB
          log.info($taskID)
          implicit val mat = ctx.components.actorMaterializer
          implicit val rA = implicitly[tasks.queue.Deserializer[$a]]
          implicit val rB = implicitly[tasks.queue.Deserializer[$b]]
          
          implicit val w = implicitly[tasks.queue.Serializer[($a,$b)]]

          implicit val r2 = implicitly[tasks.queue.Deserializer[(Int,Either[$a,$b])]]
          implicit val w2 = implicitly[tasks.queue.Serializer[(Int,Either[$a,$b])]]

          implicit val fmt = tasks.collection.EColl.flatJoinFormat[Either[$a,$b]]
          implicit val fmt2 = tasks.collection.EColl.flatJoinFormat[(Int,Either[$a,$b])]

          
          implicit val sk = new flatjoin.StringKey[Either[$a,$b]] { def key(t:Either[$a,$b]) = t match {
            case Left(a) => funA(a)
            case Right(b) => funB(b)
          }}
       
          val catted : akka.stream.scaladsl.Source[(Int,Either[$a,$b]),_] = as.source.map(a => 0 -> Left[$a,$b](a)) ++ bs.source.map(b => 1 -> Right[$a,$b](b))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(2,resourceAllocated.cpu))
          val unzippedSource = joinedSource.map{ seq =>
            val a = seq(0)
            val b = seq(1)
            (a.map(_.left.get),b.map(_.right.get))
          }.collect{
            case (Some(a),Some(b)) => (a,b)
          }
           EColl.fromSource(unzippedSource, $taskID+"." +as.basename+".x."+bs.basename,$partitionSize)(w,ctx.components)
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
          log.info($taskID)
          val fun = $fun
          val z = $zero
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[$b]]
           t.source(r,ctx.components).runFold(z)(fun)
        }
    """
    r
  }

  def reduceMacro[A: cxt.WeakTypeTag](cxt: Context)(taskID: cxt.Expr[String],
                                                    taskVersion: cxt.Expr[Int])(
      fun: cxt.Expr[(A, A) => A]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], $a]("reduce-"+$taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,EColl[$a]), $a]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer
            log.info($taskID)
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
