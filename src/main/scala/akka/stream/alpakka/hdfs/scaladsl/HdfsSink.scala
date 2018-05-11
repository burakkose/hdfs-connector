package akka.stream.alpakka.hdfs.scaladsl

import akka.Done
import akka.stream.alpakka.hdfs.HdfsWritingSettings
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
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): Sink[ByteString, Future[Done]] =
    HdfsFlow
      .data(fs, syncStrategy, rotationStrategy, settings)
      .toMat(Sink.ignore)(Keep.right)

  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Long, Long) => Path,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Sink[ByteString, Future[Done]] =
    HdfsFlow
      .compressed(fs,
                  syncStrategy,
                  rotationStrategy,
                  compressionType,
                  compressionCodec,
                  settings)
      .toMat(Sink.ignore)(Keep.right)

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Long, Long) => Path,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Sink[(K, V), Future[Done]] =
    HdfsFlow
      .sequence[K, V](
        fs,
        syncStrategy,
        rotationStrategy,
        compressionType,
        compressionCodec,
        settings,
        classK,
        classV
      )
      .toMat(Sink.ignore)(Keep.right)

}
