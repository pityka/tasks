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
            EColl.fromSource(t.source(1)(r,ctx.components),t.basename+"."+$taskID, $partitionSize, resourceAllocated.cpu)(w,ctx.components)
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
            EColl.fromSourceAsPartition(t.sourceOfPartition(idx)(r,ctx.components).map(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
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
            EColl.fromSourceAsPartition(t.sourceOfPartition(idx)(r,ctx.components).collect(fun), t.basename+"."+$taskID, idx)(w,ctx.components)
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
         EColl.fromSourceAsPartition(t.sourceOfPartition(idx)(r,ctx.components).filter(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
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
                         C: cxt.WeakTypeTag](cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int])(nameFun: cxt.Expr[B => String])(
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
        val nameFun = $nameFun
        val dataDependentName = nameFun(b)
        log.info($taskID+"-"+idx)
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$c]]
         EColl.fromSourceAsPartition(fun(t.sourceOfPartition(idx)(r,ctx.components),b)(ctx), t.basename+"."+$taskID+"."+dataDependentName, idx)(w,ctx.components)
      }

    val nameFun = $nameFun
    val dataDependentName = nameFun(b)

    releaseResources
    scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
      subtask((i,t,b))(CPUMemoryRequest(1,resourceAllocated.memory))
    }).map(_.reduce(_ ++ _)).flatMap(_.writeLength(t.basename+"."+$taskID+"."+dataDependentName))
   }

    """
    r
  }

  def mapFullSourceWithMacro[A: cxt.WeakTypeTag,
                             B: cxt.WeakTypeTag,
                             C: cxt.WeakTypeTag](cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int])(nameFun: cxt.Expr[B => String])(
      partitionSize: cxt.Expr[Long],
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

      val nameFun = $nameFun
      val dataDependentName = nameFun(b)
      val fun = $fun
      log.info($taskID)
      val r = implicitly[tasks.queue.Deserializer[$a]]
      val w = implicitly[tasks.queue.Serializer[$c]]

      EColl.fromSource(fun(t.source(resourceAllocated.cpu)(r,ctx.components),b)(ctx), t.basename+"."+$taskID+"."+dataDependentName, $partitionSize, resourceAllocated.cpu)(w,ctx.components)
    }

    """
    r
  }

  def scanMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag, C: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      nameFun: cxt.Expr[B => String])(
      partitionSize: cxt.Expr[Long],
      prepareB: cxt.Expr[
        B => tasks.queue.ComputationEnvironment => scala.concurrent.Future[C]],
      fun: cxt.Expr[(A, C) => C]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]
    val c = weakTypeOf[C]

    val r = q"""
    tasks.AsyncTask[(EColl[$a],$b), EColl[$c]]($taskID, $taskVersion) { case (t,b) => implicit ctx =>

      val nameFun = $nameFun
      val dataDependentName = nameFun(b)
      val scanFun = $fun
      val prepareFun = $prepareB
      log.info($taskID)
      val r = implicitly[tasks.queue.Deserializer[$a]]
      val w = implicitly[tasks.queue.Serializer[$c]]
      val zeroCFuture = prepareFun(b)(ctx)
      zeroCFuture.flatMap{ zeroC =>
        val scanned = t.source(resourceAllocated.cpu)(r,ctx.components).scan(zeroC)(scanFun)
        EColl.fromSource(scanned,t.basename+"."+$taskID+"."+dataDependentName, $partitionSize, resourceAllocated.cpu)(w,ctx.components)
      }(scala.concurrent.ExecutionContext.Implicits.global)
     
    }

    """
    r
  }

  def mapElemWithMacro[A: cxt.WeakTypeTag,
                       B: cxt.WeakTypeTag,
                       C: cxt.WeakTypeTag,
                       D: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      fun1: cxt.Expr[
        B => tasks.queue.ComputationEnvironment => scala.concurrent.Future[C]]
  )(fun2: cxt.Expr[(A, C) => D]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]
    val d = weakTypeOf[D]

    val r = q"""
    tasks.AsyncTask[(EColl[$a],$b), EColl[$d]]($taskID, $taskVersion) { case (t,b) => implicit ctx =>

      val subtask = tasks.AsyncTask[(Int,EColl[$a], $b), EColl[$d]]("sub-"+$taskID, $taskVersion) { case (idx,t,b) => implicit ctx =>
        val fun1 = $fun1
        val fun2 = $fun2
        log.info($taskID+"-"+idx)
        val r = implicitly[tasks.queue.Deserializer[$a]]
        val w = implicitly[tasks.queue.Serializer[$d]]
        val futureC = fun1(b)(ctx)
        futureC.flatMap{ c =>
          EColl.fromSourceAsPartition(t.sourceOfPartition(idx)(r,ctx.components).map(t => fun2((t,c))), t.basename+"."+$taskID, idx)(w,ctx.components)
        }
         
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
         EColl.fromSourceAsPartition(t.sourceOfPartition(idx)(r,ctx.components).mapConcat(x => fun(x)), t.basename+"."+$taskID, idx)(w,ctx.components)
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
      fun: cxt.Expr[A => String]) = {
    import cxt.universe._
    val a = weakTypeOf[A]

    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[$a]]($taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,EColl[$a]), EColl[$a]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
            val fun = $fun
            log.info($taskID+"-"+idx)
            implicit val mat = ctx.components.actorMaterializer
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
            implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}
            val sortedSource = t.sourceOfPartition(idx)(r,ctx.components).via(flatjoin_akka.sort)
             EColl.fromSourceAsPartition(sortedSource, t.basename+"."+$taskID, idx)(w,ctx.components)
          }

        releaseResources
        scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
          subtask((i, t))(CPUMemoryRequest(1,resourceAllocated.memory))
        }).flatMap{ partitions =>
          if (partitions.size == 1) scala.concurrent.Future.successful(partitions.head)
          else {
            val fun = $fun
            implicit val mat = ctx.components.actorMaterializer
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
            implicit val fmt = tasks.collection.EColl.flatJoinFormat[$a]
            implicit val sk = new flatjoin.StringKey[$a] { def key(t:$a) = fun(t)}

            val fileReader = (f:java.io.File) => tasks.collection.EColl.decodeFileForFlatJoin(r,sk,resourceAllocated.cpu)(f)(ctx.components.actorMaterializer.executionContext)

            scala.concurrent.Future.sequence(partitions.map(_.partitions.head.file)).flatMap{ files =>
              val mergeFlow = flatjoin_akka.merge[$a](fileReader)
              EColl.fromSource(akka.stream.scaladsl.Source(files.toList).via(mergeFlow),t.basename+"."+$taskID,$partitionSize, resourceAllocated.cpu)(w,ctx.components)
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
      fun: cxt.Expr[A => String],
      maxParallelJoins: cxt.Expr[Option[Int]]) = {
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
          val parallelismOfJoin = math.min(resourceAllocated.cpu,$maxParallelJoins.getOrElse(resourceAllocated.cpu))
          val groupedSource = t.source(resourceAllocated.cpu)(r,ctx.components).via(flatjoin_akka.groupByShardsInMemory(resourceAllocated.cpu,parallelismOfJoin))
           EColl.fromSource(groupedSource, (t.basename+"."+$taskID),$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def groupBySortedMacro[A: cxt.WeakTypeTag](
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
          val groupedSource = t.source(resourceAllocated.cpu)(r,ctx.components).via(flatjoin_akka.adjacentSpan[$a])
           EColl.fromSource(groupedSource, (t.basename+"."+$taskID),$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinByMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      fun: cxt.Expr[A => String],
      maxParallelJoins: cxt.Expr[Option[Int]]) = {
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
          val parallelismOfJoin = math.min(resourceAllocated.cpu,$maxParallelJoins.getOrElse(resourceAllocated.cpu))
          val catted = akka.stream.scaladsl.Source(ts.zipWithIndex).flatMapConcat(x => x._1.source(resourceAllocated.cpu).map(y =>x._2 -> y))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(ts.size,resourceAllocated.cpu,parallelismOfJoin))
          EColl.fromSource(joinedSource, ($taskID+"."+ts.map(_.basename).mkString(".x.")),$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinBySortedMacro[A: cxt.WeakTypeTag](
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
                    
          val catted = akka.stream.scaladsl.Source(ts.zipWithIndex).flatMapConcat(x => x._1.source(resourceAllocated.cpu).map(y =>x._2 -> y))
          val joinedSource = catted.via(flatjoin_akka.outerJoinSorted(ts.size))
          EColl.fromSource(joinedSource, ($taskID+"."+ts.map(_.basename).mkString(".x.")),$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinBy2Macro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      funA: cxt.Expr[A => String],
      funB: cxt.Expr[B => String],
      maxParallelJoins: cxt.Expr[Option[Int]]) = {
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

          val parallelismOfJoin = math.min(resourceAllocated.cpu,$maxParallelJoins.getOrElse(resourceAllocated.cpu))

          implicit val sk = new flatjoin.StringKey[Either[$a,$b]] { def key(t:Either[$a,$b]) = t match {
            case Left(a) => funA(a)
            case Right(b) => funB(b)
          }}
       
          val catted : akka.stream.scaladsl.Source[(Int,Either[$a,$b]),_] = as.source(resourceAllocated.cpu).map(a => 0 -> Left[$a,$b](a)) ++ bs.source(resourceAllocated.cpu).map(b => 1 -> Right[$a,$b](b))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(2,resourceAllocated.cpu,parallelismOfJoin))
          val unzippedSource = joinedSource.map{ seq =>
            val a = seq(0)
            val b = seq(1)
            (a.map(_.left.get),b.map(_.right.get))
          }
           EColl.fromSource(unzippedSource, $taskID+"."+as.basename+".x."+bs.basename, $partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def outerJoinBy2SortedMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
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
       
          val catted : akka.stream.scaladsl.Source[(Int,Either[$a,$b]),_] = as.source(resourceAllocated.cpu).map(a => 0 -> Left[$a,$b](a)) ++ bs.source(resourceAllocated.cpu).map(b => 1 -> Right[$a,$b](b))
          val joinedSource = catted.via(flatjoin_akka.outerJoinSorted(2,resourceAllocated.cpu))
          val unzippedSource = joinedSource.map{ seq =>
            val a = seq(0)
            val b = seq(1)
            (a.map(_.left.get),b.map(_.right.get))
          }
           EColl.fromSource(unzippedSource, $taskID+"."+as.basename+".x."+bs.basename, $partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def innerJoinBy2Macro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      partitionSize: cxt.Expr[Long],
      funA: cxt.Expr[A => String],
      funB: cxt.Expr[B => String],
      maxParallelJoins: cxt.Expr[Option[Int]]) = {
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

          val parallelismOfJoin = math.min(resourceAllocated.cpu,$maxParallelJoins.getOrElse(resourceAllocated.cpu))

          implicit val sk = new flatjoin.StringKey[Either[$a,$b]] { def key(t:Either[$a,$b]) = t match {
            case Left(a) => funA(a)
            case Right(b) => funB(b)
          }}
       
          val catted : akka.stream.scaladsl.Source[(Int,Either[$a,$b]),_] = as.source(resourceAllocated.cpu).map(a => 0 -> Left[$a,$b](a)) ++ bs.source(resourceAllocated.cpu).map(b => 1 -> Right[$a,$b](b))
          val joinedSource = catted.via(flatjoin_akka.outerJoinByShards(2,resourceAllocated.cpu,parallelismOfJoin))
          val unzippedSource = joinedSource.map{ seq =>
            val a = seq(0)
            val b = seq(1)
            (a.map(_.left.get),b.map(_.right.get))
          }.collect{
            case (Some(a),Some(b)) => (a,b)
          }
           EColl.fromSource(unzippedSource, $taskID+"." +as.basename.take(10)+as.basename.hashCode+".x."+bs.basename.take(10)+bs.basename.hashCode,$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def takeMacro[A: cxt.WeakTypeTag](cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int])(partitionSize: cxt.Expr[Long]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val r = q"""
        tasks.AsyncTask[(EColl[$a],Int), Ecoll[$a]]($taskID, $taskVersion) { case (t,n) => implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          log.info($taskID)
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[$a]]

          EColl.fromSource(t.source(resourceAllocated.cpu)(r,ctx.components).take(n),$taskID+"." +t.basename+".take."+n,$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def uniqueSortedMacro[A: cxt.WeakTypeTag](cxt: Context)(
      taskID: cxt.Expr[String],
      taskVersion: cxt.Expr[Int])(partitionSize: cxt.Expr[Long]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val r = q"""
        tasks.AsyncTask[EColl[$a], EColl[$a]]($taskID, $taskVersion) { case t=> implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          log.info($taskID)
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[$a]]

          EColl.fromSource(t.source(resourceAllocated.cpu)(r,ctx.components).statefulMapConcat(() => {
            var last : Option[$a] = None
            (elem:$a) => {
              if (last.isDefined && elem == last.get) Nil
              else {
                last = Some(elem)
                List(elem)
              }
            }
          }),$taskID+"." +t.basename+".unique",$partitionSize, resourceAllocated.cpu)(w,ctx.components)
        }
    """
    r
  }

  def toSeqMacro[A: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int]) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val r = q"""
        tasks.AsyncTask[EColl[$a], EValue[Seq[$a]]]($taskID, $taskVersion) { case t => implicit ctx =>
          implicit val mat = ctx.components.actorMaterializer
          log.info($taskID)
          val r = implicitly[tasks.queue.Deserializer[$a]]
          val w = implicitly[tasks.queue.Serializer[Seq[$a]]]
          t.source(resourceAllocated.cpu)(r,ctx.components).runWith(akka.stream.scaladsl.Sink.seq).flatMap{ seq =>
            EValue.apply[Seq[$a]](seq,$taskID+"."+t.basename)(w,ctx.components)
          }
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
        tasks.AsyncTask[EColl[$a], EValue[$a]]("reduce-"+$taskID, $taskVersion) { t => implicit ctx =>

          val subtask = tasks.AsyncTask[(Int,EColl[$a]), EValue[$a]]("sub-"+$taskID, $taskVersion) { case (idx,t) => implicit ctx =>
            implicit val mat = ctx.components.actorMaterializer
            log.info($taskID)
            val fun = $fun
            val r = implicitly[tasks.queue.Deserializer[$a]]
            val w = implicitly[tasks.queue.Serializer[$a]]
             t.sourceOfPartition(idx)(r,ctx.components).runReduce(fun).flatMap(a => EValue.apply(a,$taskID+"."+t.basename+"."+idx)(w,ctx.components))
          }

        releaseResources

        val fun = $fun
        val w = implicitly[tasks.queue.Serializer[$a]]
        val r = implicitly[tasks.queue.Deserializer[$a]]
        scala.concurrent.Future.sequence(0 until t.partitions.size map { i =>
          subtask(i -> t)(CPUMemoryRequest(resourceAllocated.cpu,resourceAllocated.memory)).flatMap(_.get(r,ctx.components))
        }).map(_.reduce((x,y) => fun(x,y)))
        .flatMap( a => EValue.apply(a,$taskID+"."+t.basename)(w,ctx.components))
       }
    """
    r
  }

  def foldMacro[A: cxt.WeakTypeTag, B: cxt.WeakTypeTag, C: cxt.WeakTypeTag](
      cxt: Context)(taskID: cxt.Expr[String], taskVersion: cxt.Expr[Int])(
      nameFun: cxt.Expr[B => String])(
      prepareB: cxt.Expr[
        B => tasks.queue.ComputationEnvironment => scala.concurrent.Future[C]],
      fun: cxt.Expr[(A, C) => C]
  ) = {
    import cxt.universe._
    val a = weakTypeOf[A]
    val b = weakTypeOf[B]
    val c = weakTypeOf[C]

    val r = q"""
    tasks.AsyncTask[(EColl[$a],$b), EValue[$c]]($taskID, $taskVersion) { case (t,b) => implicit ctx =>

      val nameFun = $nameFun
      val dataDependentName = nameFun(b)
      val foldFun = $fun
      val prepareFun = $prepareB
      log.info($taskID)
      val r = implicitly[tasks.queue.Deserializer[$a]]
      val w = implicitly[tasks.queue.Serializer[$c]]
      val zeroCFuture = prepareFun(b)(ctx)
      zeroCFuture.flatMap{ zeroC =>
        implicit val mat = ctx.components.actorMaterializer
        t.source(resourceAllocated.cpu)(r,ctx.components).runFold(zeroC)(foldFun).flatMap{ folded =>
          EValue.apply(folded,t.basename+"."+$taskID+"."+dataDependentName)(w,ctx.components)
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)
     
    }

    """
    r
  }

}
