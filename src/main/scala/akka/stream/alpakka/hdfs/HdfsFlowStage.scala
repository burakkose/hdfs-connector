package akka.stream.alpakka.hdfs

import java.io.Closeable

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.HdfsFlowLogic.{FlowState, Runner1}
import akka.stream.alpakka.hdfs.scaladsl.RotationStrategy.TimedRotationStrategy
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import cats.data.State
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Syncable, Path => HadoopPath}
import org.apache.hadoop.io.{SequenceFile, Writable}

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
    new HdfsFlowLogic.ByteLogic(fs, dest, syncStrategy, rotationStrategy, settings, outputFileGenerator, in, out, shape)
}

/**
 * Internal API
 */
private sealed abstract class HdfsFlowLogic[W <: Syncable with Closeable, I](
    fs: FileSystem,
    dest: String,
    initialSyncStrategy: SyncStrategy,
    initialRotationStrategy: RotationStrategy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    inlet: Inlet[I],
    outlet: Outlet[Future[WriteLog]],
    shape: FlowShape[I, Future[WriteLog]]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var state = {
    val initialFile = createOutputFile(0)
    FlowState(fs, createOutput(initialFile), initialFile, initialRotationStrategy, initialSyncStrategy)
  }

  protected def write(input: I): Runner1[W, Long]

  protected def createOutput(file: HadoopPath): W

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

  private def createOutputFile(c: Int): HadoopPath =
    outputFileGenerator(c, System.currentTimeMillis / 1000)

  private def rotateOutput(s: FlowState[W]): FlowState[W] = {
    s.output.close()

    val newRotationCount = s.rotationCount + 1
    val newFile = createOutputFile(newRotationCount)
    val newOutput = createOutput(newFile)
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

  private def onPushProgram(input: I) =
    for {
      offset <- write(input)
      _ <- calculateSync(null, offset)
      _ <- calculateRotation(offset)
      _ <- trySyncOutput
      _ <- tryRotateOutput
    } yield tryPull()

  private def calculateRotation(offset: Long): Runner1[W, RotationStrategy] =
    State[FlowState[W], RotationStrategy] { state =>
      val newRotation = state.rotationStrategy.calculate(offset)
      (state.copy(rotationStrategy = newRotation), newRotation)
    }

  private def calculateSync(bytes: Array[Byte], offset: Long): Runner1[W, SyncStrategy] =
    State[FlowState[W], SyncStrategy] { state =>
      val newSync = state.syncStrategy.calculate(bytes, offset)
      (state.copy(syncStrategy = newSync), newSync)
    }

  private def tryRotateOutput: Runner1[W, Boolean] =
    State[FlowState[W], Boolean] { state =>
      if (state.rotationStrategy.canRotate) {
        (rotateOutput(state), true)
      } else {
        (state, false)
      }
    }

  private def trySyncOutput: Runner1[W, Boolean] =
    State[FlowState[W], Boolean] { state =>
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

  type Runner0[W] = Runner1[W, Unit]
  type Runner1[W, A] = State[FlowState[W], A]

  sealed trait LogicState
  object LogicState {
    case object Idle extends LogicState
    case object Writing extends LogicState
    case object Finished extends LogicState
  }

  case class FlowState[W](
      offset: Long,
      rotationCount: Int,
      output: W,
      file: HadoopPath,
      rotationStrategy: RotationStrategy,
      syncStrategy: SyncStrategy,
      logicState: LogicState,
  )

  object FlowState {
    def apply[W](fs: FileSystem, output: W, file: HadoopPath, rs: RotationStrategy, ss: SyncStrategy): FlowState[W] =
      new FlowState[W](0, 0, output, file, rs, ss, LogicState.Idle)
  }

  final class ByteLogic(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsSinkSettings,
      outputFileGenerator: (Int, Long) => HadoopPath,
      inlet: Inlet[ByteString],
      outlet: Outlet[Future[WriteLog]],
      shape: FlowShape[ByteString, Future[WriteLog]]
  ) extends HdfsFlowLogic[FSDataOutputStream, ByteString](
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        settings,
        outputFileGenerator,
        inlet,
        outlet,
        shape
      ) {
    protected def createOutput(file: HadoopPath): FSDataOutputStream =
      fs.create(file)

    protected def write(input: ByteString): Runner1[FSDataOutputStream, Long] =
      State[FlowState[FSDataOutputStream], Long] { state =>
        val bytes = input.toArray
        val newOffset = state.offset + bytes.length
        state.output.write(bytes)
        (state.copy(offset = newOffset), newOffset)
      }
  }

  final class SequenceLogic[K <: Writable, V <: Writable](
      fs: FileSystem,
      conf: Configuration,
      ops: Seq[SequenceFile.Writer.Option],
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsSinkSettings,
      outputFileGenerator: (Int, Long) => HadoopPath,
      inlet: Inlet[(K, V)],
      outlet: Outlet[Future[WriteLog]],
      shape: FlowShape[(K, V), Future[WriteLog]]
  ) extends HdfsFlowLogic[SequenceFile.Writer, (K, V)](
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        settings,
        outputFileGenerator,
        inlet,
        outlet,
        shape
      ) {
    protected def createOutput(file: HadoopPath): SequenceFile.Writer =
      SequenceFile.createWriter(conf, ops: _*)

    protected def write(input: (K, V)): Runner1[SequenceFile.Writer, Long] =
      State[FlowState[SequenceFile.Writer], Long] { state =>
        state.output.append(input._1, input._2)
        val newOffset = state.output.getLength
        (state.copy(offset = newOffset), newOffset)
      }
  }

}
