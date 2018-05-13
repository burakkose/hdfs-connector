package akka.stream.alpakka.hdfs.writer

import akka.stream.alpakka.hdfs.scaladsl.FilePathGenerator
import akka.stream.alpakka.hdfs.writer.HdfsWriter.{createTargetPath, NewLineByteArray}
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

private[writer] final case class DataWriter(
    fs: FileSystem,
    pathGenerator: FilePathGenerator,
    maybeTargetPath: Option[Path],
    overwrite: Boolean
) extends HdfsWriter[FSDataOutputStream, ByteString] {
  protected val target: Path = maybeTargetPath.getOrElse(createTargetPath(pathGenerator, 0))

  def sync(): Unit = output.hsync()

  def write(input: ByteString, addNewLine: Boolean): Long = {
    val bytes = input.toArray
    output.write(bytes)
    if (addNewLine)
      output.write(NewLineByteArray)
    output.size()
  }

  def rotate(rotationCount: Long): DataWriter = {
    output.close()
    copy(maybeTargetPath = Some(createTargetPath(pathGenerator, rotationCount)))
  }

  protected def create(fs: FileSystem, file: Path): FSDataOutputStream = fs.create(file, overwrite)
}

private[hdfs] object DataWriter {
  def apply(fs: FileSystem, pathGenerator: FilePathGenerator, overwrite: Boolean): DataWriter =
    new DataWriter(fs, pathGenerator, None, overwrite)
}
