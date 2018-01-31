package akka.stream.alpakka.hdfs.scaladsl

import scala.concurrent.duration.FiniteDuration

sealed abstract class FileUnit(val byteCount: Long)

object FileUnit {
  final case object KB extends FileUnit(Math.pow(2, 10).toLong)
  final case object MB extends FileUnit(Math.pow(2, 20).toLong)
  final case object GB extends FileUnit(Math.pow(2, 30).toLong)
  final case object TB extends FileUnit(Math.pow(2, 40).toLong)
}

sealed trait RotationStrategy {
  def tryRotate(offset: Long)(fn: () => Unit): Unit
  def reset(): Unit
}

object RotationStrategy {

  final case class SizeRotationStrategy(count: Double, unit: FileUnit) extends RotationStrategy {
    private var lastOffset = 0L
    private var currentBytesWritten = 0L
    private val maxBytes = count * unit.byteCount

    def tryRotate(offset: Long)(fn: () => Unit): Unit = {
      val diff = offset - lastOffset
      currentBytesWritten += diff
      lastOffset = offset
      if (currentBytesWritten >= maxBytes)
        fn()
    }

    def reset(): Unit = {
      lastOffset = 0
      currentBytesWritten = 0
    }
  }

  final case class BufferRotationStrategy(size: Int) extends RotationStrategy {
    private var currentMessageWritten = 0

    def tryRotate(offset: Long)(fn: () => Unit): Unit = {
      currentMessageWritten += 1
      if (currentMessageWritten > size)
        fn()
    }

    def reset(): Unit =
      currentMessageWritten = 0
  }

  final case class TimedRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
    def tryRotate(offset: Long)(fn: () => Unit): Unit = ()
    def reset(): Unit = ()
  }

  final case class NoRotationStrategy() extends RotationStrategy {
    def tryRotate(offset: Long)(fn: () => Unit): Unit = ()
    def reset(): Unit = ()
  }

}
