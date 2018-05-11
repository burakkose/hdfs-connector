package akka.stream.alpakka.hdfs

import scala.concurrent.duration._

final case class HdfsWritingSettings(
    overwrite: Boolean = true,
    newLine: Boolean = false,
    initialDelay: FiniteDuration = 0.millis,
    delay: FiniteDuration = 0.millis,
) {
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings = copy(overwrite = overwrite)
  def withInitialDelay(initialDelay: FiniteDuration): HdfsWritingSettings = copy(initialDelay = initialDelay)
  def withDelay(delay: FiniteDuration): HdfsWritingSettings = copy(delay = delay)
}

object HdfsWritingSettings {

  /**
   * Java API
   */
  def create(): HdfsWritingSettings = HdfsWritingSettings()
}
