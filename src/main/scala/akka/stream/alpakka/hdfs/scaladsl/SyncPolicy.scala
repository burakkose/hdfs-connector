package akka.stream.alpakka.hdfs.scaladsl

trait SyncPolicy {
  def shouldSync(bytes: Array[Byte], offset: Long): Boolean
  def reset(): Unit
}

case class CountSyncPolicy(count: Int) extends SyncPolicy {
  private var executeCount = 0

  override def shouldSync(bytes: Array[Byte], offset: Long): Boolean = {
    executeCount += 1
    executeCount >= count
  }

  override def reset(): Unit =
    executeCount = 0
}
