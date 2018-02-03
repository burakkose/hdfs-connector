package akka.stream.alpakka.hdfs.javadsl

import java.util.function.BiFunction

import akka.NotUsed
import akka.stream.alpakka.hdfs.WriteLog
import akka.stream.alpakka.hdfs.scaladsl.{RotationStrategy, SyncStrategy, HdfsFlow => ScalaHdfsFlow}
import akka.stream.javadsl
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsFlow {

  def create(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: BiFunction[Int, Long, Path],
      settings: HdfsSinkSettings
  ): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    ScalaHdfsFlow
      .create(
        fs,
        dest,
        syncStrategy,
        rotationStrategy,
        (rc, t) => outputFileGenerator.apply(rc, t),
        settings.asScala
      )
      .asJava

  def create(fs: FileSystem,
             dest: String,
             rotationStrategy: RotationStrategy,
             outputFileGenerator: BiFunction[Int, Long, Path],
             settings: HdfsSinkSettings): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    create(fs, dest, SyncStrategy.no, rotationStrategy, outputFileGenerator, settings)

  def create(fs: FileSystem,
             dest: String,
             outputFileGenerator: BiFunction[Int, Long, Path],
             settings: HdfsSinkSettings): javadsl.Flow[ByteString, WriteLog, NotUsed] =
    create(fs, dest, RotationStrategy.no, outputFileGenerator, settings)

}
