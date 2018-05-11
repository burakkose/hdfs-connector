package akka.stream.alpakka.hdfs

import java.util.function.BiFunction

import org.apache.hadoop.fs.Path

import scala.concurrent.duration._

final case class HdfsWritingSettings(
    destination: String = "/tmp",
    overwrite: Boolean = true,
    newLine: Boolean = false,
    initialDelay: FiniteDuration = 0.millis,
    outputFileGenerator: (Long, Long) => Path = (rc, _) => new Path(s"/tmp/alpakka/$rc")
) {
  def withDestination(destination: String): HdfsWritingSettings =
    copy(destination = destination)
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings =
    copy(overwrite = overwrite)
  def withNewLine(newLine: Boolean): HdfsWritingSettings =
    copy(newLine = newLine)
  def withInitialDelay(initialDelay: FiniteDuration): HdfsWritingSettings =
    copy(initialDelay = initialDelay)
  def withOutputFileGenerator(fn: BiFunction[Long, Long, Path]): HdfsWritingSettings =
    copy(outputFileGenerator = (rc, t) => fn.apply(rc, t))
}

object HdfsWritingSettings {

  /**
   * Java API
   */
  def create(): HdfsWritingSettings = HdfsWritingSettings()
}
