package akka.stream.alpakka.hdfs.scaladsl

trait SyncStrategy {
  def trySync(bytes: Array[Byte], offset: Long)(fn: () => Unit): Unit
  def reset(): Unit
}

object SyncStrategy {
  final case class CountSyncStrategy(count: Int) extends SyncStrategy {
    private var executeCount = 0

    override def trySync(bytes: Array[Byte], offset: Long)(fn: () => Unit): Unit = {
      executeCount += 1
      if (executeCount >= count)
        fn()
    }

    override def reset(): Unit =
      executeCount = 0
  }
}
