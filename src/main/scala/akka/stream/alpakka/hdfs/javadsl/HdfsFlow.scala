package akka.stream.alpakka.hdfs.javadsl

import akka.NotUsed
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy, HdfsFlow => ScalaHdfsFlow}
import akka.stream.alpakka.hdfs.{HdfsWritingSettings, WriteLog}
import akka.stream.javadsl
import akka.util.ByteString
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsFlow {

  def data(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    ScalaHdfsFlow
      .data(
        fs,
        syncStrategy,
        rotationStrategy,
        settings
      )
      .asJava

  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    ScalaHdfsFlow
      .compressed(
        fs,
        syncStrategy,
        rotationStrategy,
        compressionType,
        compressionCodec,
        settings
      )
      .asJava

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Flow[(K, V), WriteLog, NotUsed] =
    ScalaHdfsFlow
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
      .asJava

}
