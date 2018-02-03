package akka.stream.alpakka.hdfs.scaladsl

import akka.Done
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.Future

class HdfsSink {

  def create(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Sink[ByteString, Future[Done]] =
    HdfsFlow
      .create(fs, dest, syncStrategy, rotationStrategy, outputFileGenerator, settings)
      .toMat(Sink.ignore)(Keep.right)

  def create(
      fs: FileSystem,
      dest: String,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Sink[ByteString, Future[Done]] =
    create(fs, dest, SyncStrategy.no, rotationStrategy, outputFileGenerator, settings)

  def create(
      fs: FileSystem,
      dest: String,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Sink[ByteString, Future[Done]] =
    create(fs, dest, RotationStrategy.no, outputFileGenerator, settings)

}
