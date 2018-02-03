package akka.stream.alpakka.hdfs.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.BiFunction

import akka.stream.alpakka.hdfs.HdfsSinkSettings
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy}
import akka.stream.javadsl
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsSink {

  def create(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Int, Long, Path],
      settings: HdfsSinkSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    HdfsFlow
      .create(fs, dest, syncStrategy, rotationStrategy, outputFileGenerator, settings)
      .toMat(javadsl.Sink.ignore, javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  def create(
      fs: FileSystem,
      dest: String,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Int, Long, Path],
      settings: HdfsSinkSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    create(fs, dest, SyncStrategy.no, rotationStrategy, outputFileGenerator, settings)

  def create(
      fs: FileSystem,
      dest: String,
      outputFileGenerator: BiFunction[Int, Long, Path],
      settings: HdfsSinkSettings
  ): javadsl.Sink[ByteString, CompletionStage[Done]] =
    create(fs, dest, RotationStrategy.no, outputFileGenerator, settings)

}
