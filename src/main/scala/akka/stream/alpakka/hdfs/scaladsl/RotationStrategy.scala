package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration

private[scaladsl] sealed abstract class FileUnit(val byteCount: Long)
object FileUnit {
  case object KB extends FileUnit(Math.pow(2, 10).toLong)
  case object MB extends FileUnit(Math.pow(2, 20).toLong)
  case object GB extends FileUnit(Math.pow(2, 30).toLong)
  case object TB extends FileUnit(Math.pow(2, 40).toLong)
}

sealed trait RotationStrategy extends Strategy {
  type S = RotationStrategy
}
object RotationStrategy {

  /*
   * Creates [[SizeRotationStrategy]]
   */
  def sized(count: Double, unit: FileUnit): RotationStrategy = SizeRotationStrategy(0, 0, count * unit.byteCount)

  /*
   * Creates [[CountedRotationStrategy]]
   */
  def counted(size: Long): RotationStrategy = CountedRotationStrategy(0, size)

  /*
   * Creates [[TimedRotationStrategy]]
   */
  def timed(interval: FiniteDuration): RotationStrategy = TimedRotationStrategy(interval)

  /*
   * Creates [[NoRotationStrategy]]
   */
  def none: RotationStrategy = NoRotationStrategy

  private final case class SizeRotationStrategy(
      bytesWritten: Long,
      lastOffset: Long,
      maxBytes: Double
  ) extends RotationStrategy {
    def should(): Boolean = bytesWritten >= maxBytes
    def reset(): RotationStrategy = copy(bytesWritten = 0, lastOffset = 0)
    def run(offset: Long): RotationStrategy = {
      val diff = offset - lastOffset
      copy(bytesWritten = bytesWritten + diff, lastOffset = offset)
    }
  }

  private final case class CountedRotationStrategy(
      messageWritten: Long,
      size: Long
  ) extends RotationStrategy {
    def should(): Boolean = messageWritten >= size
    def reset(): RotationStrategy = copy(messageWritten = 0)
    def run(offset: Long): RotationStrategy = copy(messageWritten = messageWritten + 1)
  }

  private[hdfs] final case class TimedRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def run(offset: Long): RotationStrategy = this
  }

  private case object NoRotationStrategy extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def run(offset: Long): RotationStrategy = this
  }

}
