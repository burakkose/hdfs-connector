package akka.stream.alpakka.hdfs

import scala.concurrent.duration._
import scala.util.matching.Regex

final case class HdfsWritingSettings(
    overwrite: Boolean = true,
    bufferSize: Int = 4096,
    replication: Int = 3,
    blockSize: Int = 67108864,
    initialDelay: FiniteDuration = 0.millis,
    delay: FiniteDuration = 0.millis,
    pattern: Regex = "*".r,
    chunkSize: Int = 4096,
) {
  def withOverwrite(overwrite: Boolean): HdfsWritingSettings =
    copy(overwrite = overwrite)

  def withBufferSize(bufferSize: Int): HdfsWritingSettings =
    copy(bufferSize = bufferSize)

  def withReplication(replication: Int): HdfsWritingSettings =
    copy(replication = replication)

  def withBlockSize(blockSize: Int): HdfsWritingSettings =
    copy(blockSize = blockSize)

  def withInitialDelay(initialDelay: FiniteDuration): HdfsWritingSettings =
    copy(initialDelay = initialDelay)

  def withDelay(delay: FiniteDuration): HdfsWritingSettings =
    copy(delay = delay)

  def withPattern(pattern: Regex): HdfsWritingSettings =
    copy(pattern = pattern)

  def withChunkSize(chunkSize: Int): HdfsWritingSettings =
    copy(chunkSize = chunkSize)
}

object HdfsWritingSettings {

  /**
   * Java API
   */
  def create(): HdfsWritingSettings = HdfsWritingSettings()
}
