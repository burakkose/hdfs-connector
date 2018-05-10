package akka.stream.alpakka.hdfs.scaladsl

sealed trait SyncStrategy extends Strategy {
  type S = SyncStrategy
}
object SyncStrategy {
  def count(c: Int): SyncStrategy = CountSyncStrategy(0, c)
  def no: SyncStrategy = NoSyncStrategy

  private final case class CountSyncStrategy(
      executeCount: Int = 0,
      count: Int
  ) extends SyncStrategy {
    def should(): Boolean = executeCount >= count
    def reset(): SyncStrategy = copy(executeCount = 0)
    def run(offset: Long): SyncStrategy = copy(executeCount = executeCount + 1)
  }

  private case object NoSyncStrategy extends SyncStrategy {
    def should(): Boolean = false
    def reset(): SyncStrategy = this
    def run(offset: Long): SyncStrategy = this
  }

}
