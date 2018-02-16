package akka.stream.alpakka.hdfs

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.HDFSFlowLogic.{FlowState, FlowStep}
import akka.stream.alpakka.hdfs.scaladsl.RotationStrategy.TimedRotationStrategy
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import cats.data.State

import scala.concurrent.Future

final case class WriteLog(path: String, offset: Long, rotation: Int)

/**
 * Internal API
 */
private[hdfs] final class HDFSFlowStage[W, I](
    dest: String,
    ss: SyncStrategy,
    rs: RotationStrategy,
    settings: HdfsWritingSettings,
    hdfsWriter: HDFSWriter[W, I]
) extends GraphStage[FlowShape[I, Future[WriteLog]]] {

  private val in = Inlet[I](Logging.simpleName(this) + ".in")
  private val out = Outlet[Future[WriteLog]](Logging.simpleName(this) + ".out")
  override val shape: FlowShape[I, Future[WriteLog]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HDFSFlowLogic(dest, ss, rs, settings, hdfsWriter, in, out, shape)
}

/**
 * Internal API
 */
private final class HDFSFlowLogic[W, I](
    dest: String,
    initialSyncStrategy: SyncStrategy,
    initialRotationStrategy: RotationStrategy,
    settings: HdfsWritingSettings,
    initialHdfsWriter: HDFSWriter[W, I],
    inlet: Inlet[I],
    outlet: Outlet[Future[WriteLog]],
    shape: FlowShape[I, Future[WriteLog]]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var state = FlowState(initialHdfsWriter, initialRotationStrategy, initialSyncStrategy)

  setHandlers(inlet, outlet, this)

  def onPush(): Unit =
    state = onPushProgram(grab(inlet))
      .runS(state)
      .value

  def onPull(): Unit =
    tryPull()

  override def preStart(): Unit = {
    // Schedule timer to rotate output file
    initialRotationStrategy match {
      case timed: TimedRotationStrategy =>
        schedulePeriodicallyWithInitialDelay(NotUsed, settings.initialDelay, timed.interval)
      case _ => ()
    }
    tryPull()
  }

  override def onTimer(timerKey: Any): Unit =
    state = rotateOutput(state)

  override def onUpstreamFailure(ex: Throwable): Unit =
    failStage(ex)

  override def onUpstreamFinish(): Unit =
    completeStage()

  private def tryPull(): Unit =
    if (!isClosed(inlet) && !hasBeenPulled(inlet)) {
      pull(inlet)
    }

  private def rotateOutput(state: FlowState[W, I]): FlowState[W, I] = {
    val newRotationCount = state.rotationCount + 1
    val newRotation = state.rotationStrategy.reset()
    val newWriter = state.writer.rotate(newRotationCount)

    newWriter.moveTo(dest)
    val message = WriteLog(newWriter.currentFile.getName, state.offset, newRotationCount)
    push(outlet, Future.successful(message))

    state.copy(offset = 0, rotationCount = newRotationCount, writer = newWriter, rotationStrategy = newRotation)
  }

  private def onPushProgram(input: I) =
    for {
      offset <- write(input)
      _ <- calculateSync(offset)
      _ <- calculateRotation(offset)
      _ <- trySyncOutput
      _ <- tryRotateOutput
    } yield tryPull()

  private def write(input: I): FlowStep[W, I, Long] =
    FlowStep[W, I, Long] { state =>
      val newOffset = state.writer.write(input, state.offset)
      (state.copy(offset = newOffset), newOffset)
    }

  private def calculateRotation(offset: Long): FlowStep[W, I, RotationStrategy] =
    FlowStep[W, I, RotationStrategy] { state =>
      val newRotation = state.rotationStrategy.calculate(offset)
      (state.copy(rotationStrategy = newRotation), newRotation)
    }

  private def calculateSync(offset: Long): FlowStep[W, I, SyncStrategy] =
    FlowStep[W, I, SyncStrategy] { state =>
      val newSync = state.syncStrategy.calculate(offset)
      (state.copy(syncStrategy = newSync), newSync)
    }

  private def tryRotateOutput: FlowStep[W, I, Boolean] =
    FlowStep[W, I, Boolean] { state =>
      if (state.rotationStrategy.canRotate) {
        (rotateOutput(state), true)
      } else {
        (state, false)
      }
    }

  private def trySyncOutput: FlowStep[W, I, Boolean] =
    FlowStep[W, I, Boolean] { state =>
      if (state.syncStrategy.canSync) {
        state.writer.sync()
        val newSync = state.syncStrategy.reset()
        (state.copy(syncStrategy = newSync), true)
      } else {
        (state, false)
      }
    }

}

private[hdfs] object HDFSFlowLogic {

  type FlowStep[W, I, A] = State[FlowState[W, I], A]
  object FlowStep {
    def apply[W, I, A](f: FlowState[W, I] => (FlowState[W, I], A)): FlowStep[W, I, A] = State.apply(f)
  }

  sealed trait LogicState
  object LogicState {
    case object Idle extends LogicState
    case object Writing extends LogicState
    case object Finished extends LogicState
  }

  final case class FlowState[W, I](
      offset: Long,
      rotationCount: Int,
      writer: HDFSWriter[W, I],
      rotationStrategy: RotationStrategy,
      syncStrategy: SyncStrategy,
      logicState: LogicState,
  )

  object FlowState {
    def apply[W, I](
        writer: HDFSWriter[W, I],
        rs: RotationStrategy,
        ss: SyncStrategy
    ): FlowState[W, I] = new FlowState[W, I](0, 0, writer, rs, ss, LogicState.Idle)
  }
}
