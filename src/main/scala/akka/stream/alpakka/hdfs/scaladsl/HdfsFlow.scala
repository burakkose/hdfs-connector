package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs.{HdfsFlowStage, WriteLog}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsFlow {

  def create(
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage(
          fs,
          dest,
          syncStrategy,
          rotationStrategy,
          settings,
          outputFileGenerator
        )
      )
      .mapAsync(1)(identity)

  def create(
      fs: FileSystem,
      dest: String,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    create(fs, dest, SyncStrategy.no, rotationStrategy, outputFileGenerator, settings)

  def create(
      fs: FileSystem,
      dest: String,
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsSinkSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    create(fs, dest, RotationStrategy.no, outputFileGenerator, settings)

}
