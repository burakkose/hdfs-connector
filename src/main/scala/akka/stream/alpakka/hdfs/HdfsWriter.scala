package akka.stream.alpakka.hdfs

import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionOutputStream}
import org.apache.hadoop.io.{SequenceFile, Writable}

/**
 * Internal API
 */
private[hdfs] sealed trait HdfsWriter[W, I] {
  protected lazy val output: W = create(fs, currentFile)
  protected val newLineByteArray: Array[Byte] = ByteString(System.getProperty("line.separator")).toArray

  def sync(): Unit
  def currentFile: Path
  def write(input: I, currentOffset: Long, addNewLine: Boolean): Long
  def rotate(rotationCount: Long): HdfsWriter[W, I]
  def moveTo(destination: String): Boolean = {
    val baseDestPath = new Path(destination)
    if (!fs.exists(baseDestPath))
      fs.mkdirs(baseDestPath)
    val destPath = new Path(destination, currentFile.getName)
    fs.rename(currentFile, destPath)
  }

  protected def fs: FileSystem
  protected def create(fs: FileSystem, file: Path): W
  protected def outputFileGenerator: (Long, Long) => Path
  protected def createOutputFile(c: Long): Path =
    outputFileGenerator(c, System.currentTimeMillis / 1000)
}

private[hdfs] object HdfsWriter {
  final case class DataWriter private (
      fs: FileSystem,
      outputFileGenerator: (Long, Long) => Path,
      maybeFile: Option[Path],
      overwrite: Boolean
  ) extends HdfsWriter[FSDataOutputStream, ByteString] {
    val currentFile: Path = maybeFile.getOrElse(createOutputFile(0))

    def rotate(rotationCount: Long): DataWriter = {
      output.close()
      copy(maybeFile = Some(createOutputFile(rotationCount)))
    }

    def write(input: ByteString, currentOffset: Long, addNewLine: Boolean): Long = {
      val bytes = input.toArray
      output.write(bytes)
      val extraOffset = if (addNewLine) {
        output.write(newLineByteArray)
        newLineByteArray.length
      } else 0
      val newOffset = currentOffset + bytes.length + extraOffset
      newOffset
    }

    def sync(): Unit = output.hsync()

    protected def create(fs: FileSystem, file: Path): FSDataOutputStream = fs.create(file, overwrite)
  }

  object DataWriter {
    def apply(fs: FileSystem, outputFileGenerator: (Long, Long) => Path, overwrite: Boolean): DataWriter =
      new DataWriter(fs, outputFileGenerator, None, overwrite)
  }

  final case class CompressedDataWriter private (
      fs: FileSystem,
      compressionCodec: CompressionCodec,
      outputFileGenerator: (Long, Long) => Path,
      maybeFile: Option[Path] = None,
      overwrite: Boolean
  ) extends HdfsWriter[FSDataOutputStream, ByteString] {
    private val compressor = CodecPool.getCompressor(compressionCodec, fs.getConf)
    private val cmpOutput = compressionCodec.createOutputStream(output, compressor)

    val currentFile: Path = maybeFile.getOrElse(createOutputFile(0))

    def rotate(rotationCount: Long): CompressedDataWriter = {
      output.close()
      cmpOutput.finish()
      copy(maybeFile = Some(createOutputFile(rotationCount)))
    }

    def write(input: ByteString, currentOffset: Long, addNewLine: Boolean): Long = {
      val bytes = input.toArray
      cmpOutput.write(bytes)
      val extraOffset = if (addNewLine) {
        cmpOutput.write(newLineByteArray)
        newLineByteArray.length
      } else 0
      val newOffset = currentOffset + bytes.length + extraOffset
      newOffset
    }

    def sync(): Unit = output.hsync()

    protected def create(fs: FileSystem, file: Path): FSDataOutputStream = fs.create(file, overwrite)
  }

  object CompressedDataWriter {
    def apply(
        fs: FileSystem,
        compressionCodec: CompressionCodec,
        outputFileGenerator: (Long, Long) => Path,
        overwrite: Boolean
    ): CompressedDataWriter =
      new CompressedDataWriter(fs, compressionCodec, outputFileGenerator, None, overwrite)
  }

  final case class SequenceWriter[K <: Writable, V <: Writable] private (
      fs: FileSystem,
      writerOptions: Seq[Writer.Option],
      outputFileGenerator: (Long, Long) => Path,
      maybeFile: Option[Path] = None
  ) extends HdfsWriter[SequenceFile.Writer, (K, V)] {
    val currentFile: Path = maybeFile.getOrElse(createOutputFile(0))

    def rotate(rotationCount: Long): SequenceWriter[K, V] = {
      output.close()
      copy(maybeFile = Some(createOutputFile(rotationCount)))
    }

    def write(input: (K, V), currentOffset: Long, addNewLine: Boolean): Long = {
      output.append(input._1, input._2)
      output.getLength
    }

    def sync(): Unit = output.hsync()

    protected def create(fs: FileSystem, file: Path): SequenceFile.Writer = {
      val ops = SequenceFile.Writer.file(file) +: writerOptions
      SequenceFile.createWriter(fs.getConf, ops: _*)
    }
  }

  object SequenceWriter {
    def apply[K <: Writable, V <: Writable](
        fs: FileSystem,
        compressionType: CompressionType,
        compressionCodec: CompressionCodec,
        classK: Class[K],
        classV: Class[V],
        outputFileGenerator: (Long, Long) => Path,
    ): SequenceWriter[K, V] =
      new SequenceWriter(fs, options(compressionType, compressionCodec, classK, classV), outputFileGenerator)

    private def options[K <: Writable, V <: Writable](
        compressionType: CompressionType,
        compressionCodec: CompressionCodec,
        classK: Class[K],
        classV: Class[V]
    ): Seq[Writer.Option] = Seq(
      SequenceFile.Writer.keyClass(classK),
      SequenceFile.Writer.valueClass(classV),
      SequenceFile.Writer.compression(compressionType, compressionCodec)
    )
  }

}
