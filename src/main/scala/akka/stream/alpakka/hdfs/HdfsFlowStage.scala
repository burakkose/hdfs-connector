package akka.stream.alpakka.hdfs

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.scaladsl.{HdfsSinkSettings, RotationStrategy, SyncStrategy, TimedRotationStrategy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path => HadoopPath}

final case class WriteLog(path: String, offset: Long, rotation: Int)

final class HdfsFlowStage(
    fs: FileSystem,
    dest: String,
    syncStrategy: SyncStrategy,
    rotationStrategy: RotationStrategy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath
) extends GraphStage[FlowShape[ByteString, WriteLog]] {

  private val in = Inlet[ByteString](Logging.simpleName(this) + ".in")
  private val out = Outlet[WriteLog](Logging.simpleName(this) + ".out")
  override val shape: FlowShape[ByteString, WriteLog] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HdfsFlowLogic(fs, dest, syncStrategy, rotationStrategy, settings, outputFileGenerator, in, out, shape)
}

/**
 * Internal API
 */
private[hdfs] final class HdfsFlowLogic(
    fs: FileSystem,
    dest: String,
    syncStrategy: SyncStrategy,
    rotationStrategy: RotationStrategy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    inlet: Inlet[ByteString],
    outlet: Outlet[WriteLog],
    shape: FlowShape[ByteString, WriteLog]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var offset: Long = 0L
  private var rotationCount: Int = 0
  private var output: FSDataOutputStream = _
  private var currentFile: HadoopPath = _

  setHandlers(inlet, outlet, this)

  def onPush(): Unit = {
    val bytes = grab(inlet).toArray
    output.write(bytes)
    offset += bytes.length

    if (syncStrategy.canSync(bytes, offset)) {
      output.hsync()
      syncStrategy.reset()
    }

    if (rotationStrategy.canRotate(offset)) {
      rotateOutputFile()
      rotationStrategy.reset()
    }

    tryPull()

  }

  def onPull(): Unit =
    tryPull()

  override def preStart(): Unit = {
    currentFile = outputFileGenerator(rotationCount, System.currentTimeMillis / 1000)
    output = fs.create(currentFile)
    // Schedule timer to rotate output file
    rotationStrategy match {
      case timedPolicy: TimedRotationStrategy =>
        schedulePeriodicallyWithInitialDelay(NotUsed, settings.initialDelay, timedPolicy.interval)
      case _ => ()
    }
    pull(inlet)
  }

  override def onTimer(timerKey: Any): Unit =
    rotateOutputFile()

  override def onUpstreamFailure(ex: Throwable): Unit =
    failStage(ex)

  override def onUpstreamFinish(): Unit =
    completeStage()

  private def tryPull(): Unit =
    if (!isClosed(inlet) && !hasBeenPulled(inlet)) {
      pull(inlet)
    }

  private def rotateOutputFile(): Unit = {
    output.close() // close output file
    rotationCount = rotationCount + 1

    val newFile = outputFileGenerator(rotationCount, System.currentTimeMillis / 1000)
    output = fs.create(newFile)
    val destPath = new HadoopPath(dest, currentFile.getName)
    fs.rename(currentFile, destPath)

    val message = WriteLog(destPath.getName, offset, rotationCount)

    push(outlet, message)

    currentFile = newFile
    offset = 0
  }
}
