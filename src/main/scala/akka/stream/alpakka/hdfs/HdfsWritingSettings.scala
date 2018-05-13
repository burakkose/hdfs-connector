package akka.stream.alpakka.hdfs

import akka.stream.alpakka.hdfs.HdfsWritingSettings._
import akka.stream.alpakka.hdfs.scaladsl.FilePathGenerator

import scala.concurrent.duration._

final case class HdfsWritingSettings(
    overwrite: Boolean = true,
    newLine: Boolean = false,
    initialDelay: FiniteDuration = 0.millis,
    pathGenerator: FilePathGenerator = DefaultFilePathGenerator
) {
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings = copy(overwrite = overwrite)
  def withNewLine(newLine: Boolean): HdfsWritingSettings = copy(newLine = newLine)
  def withInitialDelay(initialDelay: FiniteDuration): HdfsWritingSettings = copy(initialDelay = initialDelay)
  def withPathGenerator(generator: FilePathGenerator): HdfsWritingSettings = copy(pathGenerator = generator)
}

object HdfsWritingSettings {

  private val DefaultFilePathGenerator: FilePathGenerator =
    FilePathGenerator.instance((rc: Long, _: Long) => s"/tmp/alpakka/$rc")

  /**
   * Java API
   */
  def create(): HdfsWritingSettings = HdfsWritingSettings()
}
