package akka.stream.alpakka.hdfs

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.hdfs.scaladsl.{HdfsSinkSettings, RotationPolicy, SyncPolicy, TimedRotationPolicy}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path => HadoopPath}

final class HdfsFlowStage(
    fs: FileSystem,
    dest: String,
    syncPolicy: SyncPolicy,
    rotationPolicy: RotationPolicy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath
) extends GraphStage[FlowShape[ByteString, HadoopPath]] {

  private val in = Inlet[ByteString](Logging.simpleName(this) + ".in")
  private val out = Outlet[HadoopPath](Logging.simpleName(this) + ".out")
  override val shape: FlowShape[ByteString, HadoopPath] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new HdfsFlowLogic(fs, dest, syncPolicy, rotationPolicy, settings, outputFileGenerator, in, out, shape)
}

/**
 * Internal API
 */
private[hdfs] final class HdfsFlowLogic(
    fs: FileSystem,
    dest: String,
    syncPolicy: SyncPolicy,
    rotationPolicy: RotationPolicy,
    settings: HdfsSinkSettings,
    outputFileGenerator: (Int, Long) => HadoopPath,
    inlet: Inlet[ByteString],
    outlet: Outlet[HadoopPath],
    shape: FlowShape[ByteString, HadoopPath]
) extends TimerGraphStageLogic(shape)
    with InHandler
    with OutHandler {

  private var offset: Long = 0L
  private var rotationCount: Int = 0
  private var out: FSDataOutputStream = _
  private var currentFile: HadoopPath = _

  setHandlers(inlet, outlet, this)

  def onPush(): Unit = {
    val bytes = grab(inlet).toArray
    out.write(bytes)
    offset += bytes.length

    if (syncPolicy.shouldSync(bytes, offset)) {
      out.hsync()
      syncPolicy.reset()
    }

    if (rotationPolicy.shouldRotate(offset)) {
      rotateOutputFile()
      offset = 0
      rotationPolicy.reset()
    }

    tryPull()

  }

  def onPull(): Unit =
    tryPull()

  override def preStart(): Unit = {
    currentFile = outputFileGenerator(rotationCount, System.currentTimeMillis / 1000)
    out = fs.create(currentFile)
    // Schedule timer to rotate output file
    // todo maybe use schedulePeriodicallyWithInitialDelay
    rotationPolicy match {
      case timedPolicy: TimedRotationPolicy =>
        schedulePeriodically(NotUsed, timedPolicy.interval)
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
    out.close() // close output file
    rotationCount = rotationCount + 1

    val newFile = outputFileGenerator(rotationCount, System.currentTimeMillis / 1000)
    out = fs.create(newFile)
    val destPath = new HadoopPath(dest, currentFile.getName)
    fs.rename(currentFile, destPath)

    push(outlet, destPath)

    currentFile = newFile
  }
}
