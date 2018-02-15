package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs.{HDFSFlowStage, HDFSWriter, HdfsWritingSettings, WriteLog}
import akka.stream.scaladsl.Flow
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
      outputFileGenerator: (Int, Long) => Path,
      settings: HdfsWritingSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HDFSFlowStage(
          fs,
          dest,
          syncStrategy,
          rotationStrategy,
          settings,
          outputFileGenerator,
          HDFSWriter.DataWriter
        )
      )
      .mapAsync(1)(identity)

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      dest: String,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      outputFileGenerator: (Int, Long) => Path,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[(K, V), WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HDFSFlowStage(
          fs,
          dest,
          syncStrategy,
          rotationStrategy,
          settings,
          outputFileGenerator,
          HDFSWriter.SequenceWriter[K, V](compressionType, compressionCodec, classK, classV)
        )
      )
      .mapAsync(1)(identity)

}
