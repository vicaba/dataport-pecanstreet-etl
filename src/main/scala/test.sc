import akka.typed.{ActorRef, ActorSystem}
import akka.typed.scaladsl.Actor
import akka.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import lasalle.dataportpecanstreet.RowCounter._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._

implicit val timeout: Timeout = Timeout(5.seconds)

private def startRowCounterActorSystem(): (ActorSystem[Nothing], ActorRef[RowCounterProtocol]) = {
  val root = Actor.deferred[Nothing] { ctx =>
    Actor.empty
  }

  val system =
    ActorSystem[Nothing](
      "Counter",
      root
    )
  val ref = Await.result(system.systemActorOf[RowCounterProtocol](rowCounterBehavior, "rowCounter"), Duration.Inf)

  (system, ref)

}

val (system, ref) = startRowCounterActorSystem()

implicit val scheduler = system.scheduler

ref ! Add(1000)

val f: Future[Long] = ref ? Report

Await.ready(f, Duration.Inf)

f.map(println)