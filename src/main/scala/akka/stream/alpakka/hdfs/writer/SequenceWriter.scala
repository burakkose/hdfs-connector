package akka.stream.alpakka.hdfs.writer

import akka.stream.alpakka.hdfs.scaladsl.FilePathGenerator
import akka.stream.alpakka.hdfs.writer.HdfsWriter.createTargetPath
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{SequenceFile, Writable}

private[writer] final case class SequenceWriter[K <: Writable, V <: Writable](
    fs: FileSystem,
    writerOptions: Seq[Writer.Option],
    pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path]
) extends HdfsWriter[SequenceFile.Writer, (K, V)] {
  protected val target: Path = maybeTargetPath.getOrElse(createTargetPath(pathGenerator, 0))

  def sync(): Unit = output.hsync()

  def write(input: (K, V), addNewLine: Boolean): Long = {
    output.append(input._1, input._2)
    output.getLength
  }

  def rotate(rotationCount: Long): SequenceWriter[K, V] = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  protected def create(fs: FileSystem, file: Path): SequenceFile.Writer = {
    val ops = SequenceFile.Writer.file(file) +: writerOptions
    SequenceFile.createWriter(fs.getConf, ops: _*)
  }
}

private[hdfs] object SequenceWriter {
  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator,
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](fs, options(classK, classV), pathGenerator, None)

  def apply[K <: Writable, V <: Writable](
      fs: FileSystem,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V],
      pathGenerator: FilePathGenerator,
  ): SequenceWriter[K, V] =
    new SequenceWriter[K, V](fs, options(compressionType, compressionCodec, classK, classV), pathGenerator, None)

  private def options[K <: Writable, V <: Writable](
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = Seq(
    SequenceFile.Writer.keyClass(classK),
    SequenceFile.Writer.valueClass(classV),
  )

  private def options[K <: Writable, V <: Writable](
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      classK: Class[K],
      classV: Class[V]
  ): Seq[Writer.Option] = SequenceFile.Writer.compression(compressionType, compressionCodec) +: options(classK, classV)
}
