package akka.stream.alpakka.hdfs.javadsl

import akka.stream.alpakka.hdfs.scaladsl.{HdfsSinkSettings => ScalaHdfsSinkSettings}

import scala.concurrent.duration._
import scala.util.matching.Regex

final class HdfsSinkSettings(
    overwrite: Boolean,
    bufferSize: Int,
    replication: Int,
    blockSize: Int,
    initialDelay: FiniteDuration,
    delay: FiniteDuration,
    pattern: Regex,
    chunkSize: Int,
) {

  def this() = this(true, 4096, 3, 67108864, 0.millis, 0.millis, "*".r, 4096)

  def withOverwrite(overwrite: Boolean): HdfsSinkSettings =
    new HdfsSinkSettings(
      overwrite,
      this.bufferSize,
      this.replication,
      this.blockSize,
      this.initialDelay,
      this.delay,
      this.pattern,
      this.chunkSize
    )

  def withBufferSize(bufferSize: Int): HdfsSinkSettings =
    new HdfsSinkSettings(
      this.overwrite,
      bufferSize,
      this.replication,
      this.blockSize,
      this.initialDelay,
      this.delay,
      this.pattern,
      this.chunkSize
    )

  def withReplication(replication: Int) =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      replication,
      this.blockSize,
      this.initialDelay,
      this.delay,
      this.pattern,
      this.chunkSize
    )

  def withBlockSize(blockSize: Int) =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      this.replication,
      blockSize,
      this.initialDelay,
      this.delay,
      this.pattern,
      this.chunkSize
    )

  def withInitialDelay(initialDelay: FiniteDuration) =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      this.replication,
      this.blockSize,
      initialDelay,
      this.delay,
      this.pattern,
      this.chunkSize
    )

  def withDelay(delay: FiniteDuration) =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      this.replication,
      this.blockSize,
      this.initialDelay,
      delay,
      this.pattern,
      this.chunkSize
    )

  def withPattern(pattern: Regex) =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      this.replication,
      this.blockSize,
      this.initialDelay,
      this.delay,
      pattern,
      this.chunkSize
    )

  def withChunkSize(chunkSize: Int): HdfsSinkSettings =
    new HdfsSinkSettings(
      this.overwrite,
      this.bufferSize,
      this.replication,
      this.blockSize,
      this.initialDelay,
      this.delay,
      this.pattern,
      chunkSize
    )

  private[javadsl] def asScala: ScalaHdfsSinkSettings =
    ScalaHdfsSinkSettings(
      overwrite = this.overwrite,
      bufferSize = this.bufferSize,
      replication = this.replication,
      blockSize = this.blockSize,
      initialDelay = this.initialDelay,
      delay = this.delay,
      pattern = this.pattern,
      chunkSize = this.chunkSize,
    )
}
