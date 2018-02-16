package akka.stream.alpakka.hdfs.javadsl

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy, HdfsFlow => ScalaHdfsFlow}
import akka.stream.alpakka.hdfs.{HdfsWritingSettings, WriteLog}
import akka.stream.javadsl
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsFlow {

  def data(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Long, Long, Path],
      settings: HdfsWritingSettings
  ): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    ScalaHdfsFlow
      .data(
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        (rc, t) => outputFileGenerator.apply(rc, t),
        settings
      )
      .asJava

  def compressed(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Long, Long, Path],
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    ScalaHdfsFlow
      .compressed(
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        (rc, t) => outputFileGenerator.apply(rc, t),
        compressionType,
        compressionCodec,
        settings
      )
      .asJava

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Long, Long, Path],
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Flow[(K, V), WriteLog, NotUsed] =
    ScalaHdfsFlow
      .sequence[K, V](
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        (rc, t) => outputFileGenerator.apply(rc, t),
        compressionType,
        compressionCodec,
        settings,
        classK,
        classV
      )
      .asJava

}
