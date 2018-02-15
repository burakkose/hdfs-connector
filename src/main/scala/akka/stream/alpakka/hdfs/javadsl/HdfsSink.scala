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

  def create(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Int, Long, Path],
      settings: HdfsWritingSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .data(fs, dest, syncStrategy, rotationStrategy, outputFileGenerator, settings)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Int, Long, Path],
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): javadsl.Sink[(K, V), CompletionStage[Done]] =
    HdfsFlow
      .sequence(
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
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

}
