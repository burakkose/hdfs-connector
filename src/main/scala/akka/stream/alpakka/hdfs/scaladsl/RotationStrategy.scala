package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration

sealed trait RotationStrategy {
  def canRotate(offset: Long): Boolean
  def reset(): Unit
}

sealed abstract class FileUnit(val byteCount: Long)
object FileUnit {
  case object KB extends FileUnit(Math.pow(2, 10).toLong)
  case object MB extends FileUnit(Math.pow(2, 20).toLong)
  case object GB extends FileUnit(Math.pow(2, 30).toLong)
  case object TB extends FileUnit(Math.pow(2, 40).toLong)
}

case class SizeRotationStrategy(count: Double, unit: FileUnit) extends RotationStrategy {
  private var lastOffset = 0L
  private var currentBytesWritten = 0L
  private val maxBytes = count * unit.byteCount

  def canRotate(offset: Long): Boolean = {
    val diff = offset - lastOffset
    currentBytesWritten += diff
    lastOffset = offset
    currentBytesWritten >= maxBytes
  }

  def reset(): Unit = {
    lastOffset = 0
    currentBytesWritten = 0
  }
}

case class TimedRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
  def canRotate(offset: Long): Boolean = false
  def reset(): Unit = ()
}

case class NoRotationStrategy() extends RotationStrategy {
  def canRotate(offset: Long): Boolean = false
  def reset(): Unit = ()
}
