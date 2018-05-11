package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs.{HDFSFlowStage, HDFSWriter, HdfsWritingSettings, WriteLog}
import akka.stream.scaladsl.Flow
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
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HDFSFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HDFSWriter.DataWriter(fs, settings.outputFileGenerator, settings.overwrite)
        )
      )
      .mapAsync(1)(identity)

  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HDFSFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HDFSWriter
            .CompressedDataWriter(fs, compressionType, compressionCodec, settings.outputFileGenerator, settings.overwrite)
        )
      )
      .mapAsync(1)(identity)

  def sequence[K <: Writable, V <: Writable](
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionType: CompressionType,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings,
      classK: Class[K],
      classV: Class[V]
  ): Flow[(K, V), WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HDFSFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HDFSWriter.SequenceWriter(fs, compressionType, compressionCodec, classK, classV, settings.outputFileGenerator)
        )
      )
      .mapAsync(1)(identity)

}
