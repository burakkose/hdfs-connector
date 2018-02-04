package akka.stream.alpakka.hdfs

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.HdfsFlowLogic.{FlowState, Runner1}
import akka.stream.alpakka.hdfs.scaladsl.RotationStrategy.TimedRotationStrategy
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import cats.data.State
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path => HadoopPath}

import scala.concurrent.Future

final case class WriteLog(path: String, offset: Long, rotation: Int)

/**
 * Internal API
 */
private[hdfs] final class HdfsFlowStage(
    fs: FileSystem,
    dest: String,
    syncStrategy: SyncStrategy,
    rotationStrategy: RotationStrategy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath
) extends GraphStage[FlowShape[ByteString, Future[WriteLog]]] {

  private val in = Inlet[ByteString](Logging.simpleName(this) + ".in")
  private val out = Outlet[Future[WriteLog]](Logging.simpleName(this) + ".out")
  override val shape: FlowShape[ByteString, Future[WriteLog]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HdfsFlowLogic(fs, dest, syncStrategy, rotationStrategy, settings, outputFileGenerator, in, out, shape)
}

/**
 * Internal API
 */
private[hdfs] final class HdfsFlowLogic(
    fs: FileSystem,
    dest: String,
    initialSyncStrategy: SyncStrategy,
    initialRotationStrategy: RotationStrategy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    inlet: Inlet[ByteString],
    outlet: Outlet[Future[WriteLog]],
    shape: FlowShape[ByteString, Future[WriteLog]]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var state = FlowState(fs, createOutputFile(0), initialRotationStrategy, initialSyncStrategy)

  setHandlers(inlet, outlet, this)

  def onPush(): Unit =
    state = onPushProgram(grab(inlet).toArray)
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

  private def createOutputFile(c: Int): HadoopPath =
    outputFileGenerator(c, System.currentTimeMillis / 1000)

  private def rotateOutput(s: FlowState): FlowState = {
    s.output.close()

    val newRotationCount = s.rotationCount + 1
    val newFile = createOutputFile(newRotationCount)
    val newOutput = fs.create(newFile)
    val destPath = new HadoopPath(dest, s.file.getName)

    fs.rename(s.file, destPath)
    val message = WriteLog(destPath.getName, s.offset, newRotationCount)
    push(outlet, Future.successful(message))

    s.copy(offset = 0,
           file = newFile,
           rotationCount = newRotationCount,
           output = newOutput,
           rotationStrategy = s.rotationStrategy.reset())
  }

  private def onPushProgram(bytes: Array[Byte]) =
    for {
      offset <- writeBytes(bytes)
      _ <- calculateSync(bytes, offset)
      _ <- calculateRotation(offset)
      _ <- trySyncOutput
      _ <- tryRotateOutput
    } yield tryPull()

  private def writeBytes(bytes: Array[Byte]): Runner1[Long] =
    State[FlowState, Long] { state =>
      val newOffset = state.offset + bytes.length
      state.output.write(bytes)
      (state.copy(offset = newOffset), newOffset)
    }

  private def calculateRotation(offset: Long): Runner1[RotationStrategy] =
    State[FlowState, RotationStrategy] { state =>
      val newRotation = state.rotationStrategy.calculate(offset)
      (state.copy(rotationStrategy = newRotation), newRotation)
    }

  private def calculateSync(bytes: Array[Byte], offset: Long): Runner1[SyncStrategy] =
    State[FlowState, SyncStrategy] { state =>
      val newSync = state.syncStrategy.calculate(bytes, offset)
      (state.copy(syncStrategy = newSync), newSync)
    }

  private def tryRotateOutput: Runner1[Boolean] =
    State[FlowState, Boolean] { state =>
      if (state.rotationStrategy.canRotate) {
        (rotateOutput(state), true)
      } else {
        (state, false)
      }
    }

  private def trySyncOutput: Runner1[Boolean] =
    State[FlowState, Boolean] { state =>
      val done = if (state.syncStrategy.canSync) {
        state.output.hsync()
        state.syncStrategy.reset()
        true
      } else {
        false
      }
      (state, done)
    }
}

private object HdfsFlowLogic {

  sealed trait HdfsLogicState

  object HdfsLogicState {
    case object Idle extends HdfsLogicState
    case object Writing extends HdfsLogicState
    case object Finished extends HdfsLogicState
  }

  type Runner0 = Runner1[Unit]
  type Runner1[A] = State[FlowState, A]

  case class FlowState(
      offset: Long,
      rotationCount: Int,
      output: FSDataOutputStream,
      file: HadoopPath,
      rotationStrategy: RotationStrategy,
      syncStrategy: SyncStrategy,
      logicState: HdfsLogicState,
  )

  object FlowState {
    def apply(fs: FileSystem, file: HadoopPath, rs: RotationStrategy, ss: SyncStrategy): FlowState =
      new FlowState(0, 0, fs.create(file), file, rs, ss, HdfsLogicState.Idle)
  }

}
