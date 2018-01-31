package akka.stream.alpakka.hdfs.scaladsl

trait SyncStrategy {
  def canSync(bytes: Array[Byte], offset: Long): Boolean
  def reset(): Unit
}

case class CountSyncStrategy(count: Int) extends SyncStrategy {
  private var executeCount = 0

  override def canSync(bytes: Array[Byte], offset: Long): Boolean = {
    executeCount += 1
    executeCount >= count
  }

  override def reset(): Unit =
    executeCount = 0
}
