package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.matching.Regex

case class HdfsSinkSettings(
    overwrite: Boolean = true,
    bufferSize: Int = 4096,
    replication: Int = 3,
    blockSize: Int = 67108864,
    initialDelay: FiniteDuration = 0.millis,
    delay: FiniteDuration = 0.millis,
    pattern: Regex = "*".r,
    chunkSize: Int = 4096,
)
