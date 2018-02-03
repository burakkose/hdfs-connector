package akka.stream.alpakka.hdfs

import scala.concurrent.duration._
import scala.util.matching.Regex

final case class HdfsSinkSettings(
    overwrite: Boolean = true,
    bufferSize: Int = 4096,
    replication: Int = 3,
    blockSize: Int = 67108864,
    initialDelay: FiniteDuration = 0.millis,
    delay: FiniteDuration = 0.millis,
    pattern: Regex = "*".r,
    chunkSize: Int = 4096,
) {
  def withOverwrite(overwrite: Boolean): HdfsSinkSettings =
    copy(overwrite = overwrite)

  def withBufferSize(bufferSize: Int): HdfsSinkSettings =
    copy(bufferSize = bufferSize)

  def withReplication(replication: Int): HdfsSinkSettings =
    copy(replication = replication)

  def withBlockSize(blockSize: Int): HdfsSinkSettings =
    copy(blockSize = blockSize)

  def withInitialDelay(initialDelay: FiniteDuration): HdfsSinkSettings =
    copy(initialDelay = initialDelay)

  def withDelay(delay: FiniteDuration): HdfsSinkSettings =
    copy(delay = delay)

  def withPattern(pattern: Regex): HdfsSinkSettings =
    copy(pattern = pattern)

  def withChunkSize(chunkSize: Int): HdfsSinkSettings =
    copy(chunkSize = chunkSize)
}

object HdfsSinkSettings {

  /**
   * Java API
   */
  def create(): HdfsSinkSettings = HdfsSinkSettings()
}
