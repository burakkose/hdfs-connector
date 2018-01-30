package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration

sealed trait RotationPolicy {
  def shouldRotate(offset: Long): Boolean
  def reset(): Unit
}

sealed abstract class FileUnit(val byteCount: Long)
object FileUnit {
  case object KB extends FileUnit(Math.pow(2, 10).toLong)
  case object MB extends FileUnit(Math.pow(2, 20).toLong)
  case object GB extends FileUnit(Math.pow(2, 30).toLong)
  case object TB extends FileUnit(Math.pow(2, 40).toLong)
}

case class SizeRotationPolicy(count: Double, unit: FileUnit) extends RotationPolicy {
  private var lastOffset = 0L
  private var currentBytesWritten = 0L
  private val maxBytes = count * unit.byteCount

  def shouldRotate(offset: Long): Boolean = {
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

case class TimedRotationPolicy(interval: FiniteDuration) extends RotationPolicy {
  def shouldRotate(offset: Long): Boolean = false
  def reset(): Unit = ()
}

case class NoRotationPolicy() extends RotationPolicy {
  def shouldRotate(offset: Long): Boolean = false
  def reset(): Unit = ()
}
