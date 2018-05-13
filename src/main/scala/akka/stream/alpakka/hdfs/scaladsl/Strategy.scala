package akka.stream.alpakka.hdfs.scaladsl

private[scaladsl] trait Strategy {
  type S <: Strategy
  def should(): Boolean
  def reset(): S
  def update(offset: Long): S
}
