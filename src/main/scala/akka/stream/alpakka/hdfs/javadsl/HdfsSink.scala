package akka.stream.alpakka.hdfs.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import akka.stream.alpakka.hdfs.HdfsWritingSettings
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.javadsl
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsSink {

  def data(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .data(fs, syncStrategy, rotationStrategy, settings)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .compressed(fs,
                  syncStrategy,
                  rotationStrategy,
                  compressionType,
                  compressionCodec,
                  settings)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Sink[(K, V), CompletionStage[Done]] =
    HdfsFlow
      .sequence(
        fs,
        syncStrategy,
        rotationStrategy,
        compressionType,
        compressionCodec,
        settings,
        classK,
        classV
      )
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
