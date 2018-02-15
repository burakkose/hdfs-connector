package akka.stream.alpakka.hdfs

import java.io.Closeable

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.HDFSFlowLogic.{FlowState, FlowStep}
import akka.stream.alpakka.hdfs.scaladsl.RotationStrategy.TimedRotationStrategy
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import cats.data.State
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Syncable, Path => HadoopPath}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

import scala.concurrent.Future

final case class WriteLog(path: String, offset: Long, rotation: Int)

/**
 * Internal API
 */
private[hdfs] final class HDFSFlowStage[W <: Syncable with Closeable, I](
    fs: FileSystem,
    dest: String,
    ss: SyncStrategy,
    rs: RotationStrategy,
    settings: HdfsWritingSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    hdfsWriter: HDFSWriter[W, I]
) extends GraphStage[FlowShape[I, Future[WriteLog]]] {

  private val in = Inlet[I](Logging.simpleName(this) + ".in")
  private val out = Outlet[Future[WriteLog]](Logging.simpleName(this) + ".out")
  override val shape: FlowShape[I, Future[WriteLog]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HDFSFlowLogic(fs, dest, ss, rs, settings, outputFileGenerator, hdfsWriter, in, out, shape)
}

/**
 * Internal API
 */
private final class HDFSFlowLogic[W <: Syncable with Closeable, I](
    fs: FileSystem,
    dest: String,
    initialSyncStrategy: SyncStrategy,
    initialRotationStrategy: RotationStrategy,
    settings: HdfsWritingSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    hdfsWriter: HDFSWriter[W, I],
    inlet: Inlet[I],
    outlet: Outlet[Future[WriteLog]],
    shape: FlowShape[I, Future[WriteLog]]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var state = {
    val initialFile = createOutputFile(0)
    FlowState(fs, hdfsWriter.create(fs, initialFile), initialFile, initialRotationStrategy, initialSyncStrategy)
  }

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

  private def rotateOutput(state: FlowState[W]): FlowState[W] = {
    state.output.close()

    val newRotationCount = state.rotationCount + 1
    val newRotation = state.rotationStrategy.reset()
    val newFile = createOutputFile(newRotationCount)
    val newOutput = hdfsWriter.create(fs, newFile)
    val destPath = new HadoopPath(dest, state.file.getName)

    fs.rename(state.file, destPath)
    val message = WriteLog(destPath.getName, state.offset, newRotationCount)
    push(outlet, Future.successful(message))

    state.copy(offset = 0,
               file = newFile,
               rotationCount = newRotationCount,
               output = newOutput,
               rotationStrategy = newRotation)
  }

  private def onPushProgram(input: I) =
    for {
      offset <- hdfsWriter.write(input)
      _ <- calculateSync(null, offset)
      _ <- calculateRotation(offset)
      _ <- trySyncOutput
      _ <- tryRotateOutput
    } yield tryPull()

  private def calculateRotation(offset: Long): FlowStep[W, RotationStrategy] =
    FlowStep[W, RotationStrategy] { state =>
      val newRotation = state.rotationStrategy.calculate(offset)
      (state.copy(rotationStrategy = newRotation), newRotation)
    }

  private def calculateSync(bytes: Array[Byte], offset: Long): FlowStep[W, SyncStrategy] =
    FlowStep[W, SyncStrategy] { state =>
      val newSync = state.syncStrategy.calculate(bytes, offset)
      (state.copy(syncStrategy = newSync), newSync)
    }

  private def tryRotateOutput: FlowStep[W, Boolean] =
    FlowStep[W, Boolean] { state =>
      if (state.rotationStrategy.canRotate) {
        (rotateOutput(state), true)
      } else {
        (state, false)
      }
    }

  private def trySyncOutput: FlowStep[W, Boolean] =
    FlowStep[W, Boolean] { state =>
      if (state.syncStrategy.canSync) {
        state.output.hsync()
        val newSync = state.syncStrategy.reset()
        (state.copy(syncStrategy = newSync), true)
      } else {
        (state.copy(), false)
      }
    }

}

private object HDFSFlowLogic {

  type FlowStep[W, A] = State[FlowState[W], A]
  object FlowStep {
    def apply[W, A](f: FlowState[W] => (FlowState[W], A)): FlowStep[W, A] = State.apply(f)
  }

  sealed trait LogicState
  object LogicState {
    case object Idle extends LogicState
    case object Writing extends LogicState
    case object Finished extends LogicState
  }

  final case class FlowState[W](
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
}

private sealed trait HDFSWriter[W <: Syncable with Closeable, I] {
  def write(input: I): FlowStep[W, Long]
  def create(fs: FileSystem, file: HadoopPath): W
}

private[hdfs] object HDFSWriter {

  case object DataWriter extends HDFSWriter[FSDataOutputStream, ByteString] {
    def create(fs: FileSystem, file: HadoopPath): FSDataOutputStream =
      fs.create(file)

    def write(input: ByteString): FlowStep[FSDataOutputStream, Long] =
      FlowStep[FSDataOutputStream, Long] { state =>
        val bytes = input.toArray
        val newOffset = state.offset + bytes.length
        state.output.write(bytes)
        (state.copy(offset = newOffset), newOffset)
      }
  }

  final case class SequenceWriter[K <: Writable, V <: Writable](
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V]
  ) extends HDFSWriter[SequenceFile.Writer, (K, V)] {
    private val DefaultOps: Seq[Writer.Option] = Seq(
      SequenceFile.Writer.keyClass(classK),
      SequenceFile.Writer.valueClass(classV),
      SequenceFile.Writer.compression(compressionType, compressionCodec)
    )

    def create(fs: FileSystem, file: HadoopPath): SequenceFile.Writer = {
      val ops = DefaultOps :+ SequenceFile.Writer.file(file)
      SequenceFile.createWriter(fs.getConf, ops: _*)
    }

    def write(input: (K, V)): FlowStep[SequenceFile.Writer, Long] =
      FlowStep[SequenceFile.Writer, Long] { state =>
        state.output.append(input._1, input._2)
        val newOffset = state.output.getLength
        (state.copy(offset = newOffset), newOffset)
      }
  }

}
