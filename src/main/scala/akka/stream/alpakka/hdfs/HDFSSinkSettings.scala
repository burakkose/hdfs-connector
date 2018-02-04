package akka.stream.alpakka.hdfs

import scala.concurrent.duration._
import scala.util.matching.Regex

final case class HDFSSinkSettings(
    overwrite: Boolean = true,
    bufferSize: Int = 4096,
    replication: Int = 3,
    blockSize: Int = 67108864,
    initialDelay: FiniteDuration = 0.millis,
    delay: FiniteDuration = 0.millis,
    pattern: Regex = "*".r,
    chunkSize: Int = 4096,
) {
  def withOverwrite(overwrite: Boolean): HDFSSinkSettings =
    copy(overwrite = overwrite)

  def withBufferSize(bufferSize: Int): HDFSSinkSettings =
    copy(bufferSize = bufferSize)

  def withReplication(replication: Int): HDFSSinkSettings =
    copy(replication = replication)

  def withBlockSize(blockSize: Int): HDFSSinkSettings =
    copy(blockSize = blockSize)

  def withInitialDelay(initialDelay: FiniteDuration): HDFSSinkSettings =
    copy(initialDelay = initialDelay)

  def withDelay(delay: FiniteDuration): HDFSSinkSettings =
    copy(delay = delay)

  def withPattern(pattern: Regex): HDFSSinkSettings =
    copy(pattern = pattern)

  def withChunkSize(chunkSize: Int): HDFSSinkSettings =
    copy(chunkSize = chunkSize)
}

object HDFSSinkSettings {

  /**
   * Java API
   */
  def create(): HDFSSinkSettings = HDFSSinkSettings()
}
