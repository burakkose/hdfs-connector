package akka.stream.alpakka.hdfs.scaladsl

sealed trait SyncStrategy {
  def calculate(offset: Long): SyncStrategy
  def canSync: Boolean
  def reset(): SyncStrategy
}

object SyncStrategy {

  def count(c: Int): SyncStrategy =
    CountSyncStrategy(0, c)

  def no: SyncStrategy =
    NoSyncStrategy()

  private[hdfs] final case class CountSyncStrategy(
      executeCount: Int = 0,
      count: Int
  ) extends SyncStrategy {
    def canSync: Boolean = executeCount >= count
    def reset(): SyncStrategy = copy(executeCount = 0)
    def calculate(offset: Long): SyncStrategy = copy(executeCount = executeCount + 1)
  }

  private[hdfs] final case class NoSyncStrategy() extends SyncStrategy {
    def canSync: Boolean = false
    def reset(): SyncStrategy = this
    def calculate(offset: Long): SyncStrategy = this
  }

}
