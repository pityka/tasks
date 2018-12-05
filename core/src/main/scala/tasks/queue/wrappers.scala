/*
 * The MIT License
 *
 * Copyright (c) 2015 ECOLE POLYTECHNIQUE FEDERALE DE LAUSANNE, Switzerland,
 * Group Fellay
 * Modified work, Copyright (c) 2016 Istvan Bartha

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

package tasks.queue

import akka.actor._

case class NodeLocalCacheActor(actor: ActorRef)

case class QueueActor(actor: ActorRef)

case class LauncherActor(actor: ActorRef)

case class Proxy(actor: ActorRef)

object Proxy {
  import io.circe._
  import io.circe.generic.semiauto._

  import tasks.wire.actorRefEncoder
  implicit val encoder: Encoder[Proxy] = deriveEncoder[Proxy]

  implicit def decoder(implicit EAS: ExtendedActorSystem): Decoder[Proxy] = {
    implicit val actorRefDecoder: Decoder[ActorRef] =
      tasks.wire.actorRefDecoder(EAS)
    val _ = actorRefDecoder // suppress unused warning
    deriveDecoder[Proxy]
  }
}

case class LauncherStopped(launcher: LauncherActor)
