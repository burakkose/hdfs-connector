package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration

sealed abstract class FileUnit(val byteCount: Long)

object FileUnit {
  case object KB extends FileUnit(Math.pow(2, 10).toLong)
  case object MB extends FileUnit(Math.pow(2, 20).toLong)
  case object GB extends FileUnit(Math.pow(2, 30).toLong)
  case object TB extends FileUnit(Math.pow(2, 40).toLong)
}

sealed trait RotationStrategy {
  def calculate(offset: Long): RotationStrategy
  def canRotate: Boolean
  def reset(): RotationStrategy
}

object RotationStrategy {

  def sized(count: Double, unit: FileUnit): RotationStrategy =
    SizeRotationStrategy(0, 0, count * unit.byteCount)

  def buffered(size: Int): RotationStrategy =
    BufferRotationStrategy(0, size)

  def timed(interval: FiniteDuration): RotationStrategy =
    TimedRotationStrategy(interval)

  def no: RotationStrategy =
    NoRotationStrategy()

  private[hdfs] final case class SizeRotationStrategy(
      bytesWritten: Long,
      lastOffset: Long,
      maxBytes: Double
  ) extends RotationStrategy {
    def canRotate: Boolean = bytesWritten >= maxBytes
    def reset(): RotationStrategy = copy(bytesWritten = 0, lastOffset = 0)
    def calculate(offset: Long): RotationStrategy = {
      val diff = offset - lastOffset
      copy(bytesWritten = bytesWritten + diff, lastOffset = offset)
    }
  }

  private[hdfs] final case class BufferRotationStrategy(
      messageWritten: Int,
      size: Int
  ) extends RotationStrategy {
    def canRotate: Boolean = messageWritten >= size
    def reset(): RotationStrategy = copy(messageWritten = 0)
    def calculate(offset: Long): RotationStrategy = copy(messageWritten = messageWritten + 1)
  }

  private[hdfs] final case class TimedRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
    def canRotate: Boolean = false
    def reset(): RotationStrategy = this
    def calculate(offset: Long): RotationStrategy = this
  }

  private[hdfs] final case class NoRotationStrategy() extends RotationStrategy {
    def canRotate: Boolean = false
    def reset(): RotationStrategy = this
    def calculate(offset: Long): RotationStrategy = this
  }

}
