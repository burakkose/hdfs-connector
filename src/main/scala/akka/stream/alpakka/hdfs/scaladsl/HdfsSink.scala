package akka.stream.alpakka.hdfs.scaladsl

import akka.Done
import akka.stream.alpakka.hdfs.HDFSSinkSettings
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

import scala.concurrent.Future

class HdfsSink {

  def data(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      settings: HDFSSinkSettings
  ): Sink[ByteString, Future[Done]] =
    HdfsFlow
      .data(fs, dest, syncStrategy, rotationStrategy, outputFileGenerator, settings)
      .toMat(Sink.ignore)(Keep.right)

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HDFSSinkSettings,
      classK: Class[K],
      classV: Class[V]
  ): Sink[(K, V), Future[Done]] =
    HdfsFlow
      .sequence[K, V](
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        outputFileGenerator,
        compressionType,
        compressionCodec,
        settings,
        classK,
        classV
      )
      .toMat(Sink.ignore)(Keep.right)

}
