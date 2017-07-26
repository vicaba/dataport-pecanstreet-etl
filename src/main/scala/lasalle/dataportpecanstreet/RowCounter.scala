package lasalle.dataportpecanstreet

import akka.typed.scaladsl.Actor
import akka.typed.{ActorRef, Behavior}

object RowCounter {

  sealed trait RowCounterProtocol

  final case class Add(n: Long) extends RowCounterProtocol

  /**
   * Not Serializable!
   *
   */
  final case class Report(sendBackTo: ActorRef[Long]) extends RowCounterProtocol

  val rowCounterBehavior: Behavior[RowCounterProtocol] = rowCounterBehavior(0)

  private def rowCounterBehavior(acc: Long): Behavior[RowCounterProtocol] =
    Actor.immutable[RowCounterProtocol] { (ctx, msg) =>
      msg match {
        case Add(n) => rowCounterBehavior(acc + n)
        case r @ Report(s) =>
          s ! acc
          rowCounterBehavior(acc)
      }
    }

}
