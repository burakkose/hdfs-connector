package akka.stream.alpakka.hdfs.scaladsl

import akka.NotUsed
import akka.stream.alpakka.hdfs.{HdfsFlowStage, HdfsWriter, HdfsWritingSettings, WriteLog}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec

object HdfsFlow {

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[FSDataOutputStream]]
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param settings Hdfs writing settings
   */
  def data(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      settings: HdfsWritingSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HdfsWriter.DataWriter(fs, settings.outputFileGenerator, settings.overwrite)
        )
      )
      .mapAsync(1)(identity)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[CompressionOutputStream]]
   *
   * @param fs HDFS FileSystem
   * @param syncStrategy Sync Strategy
   * @param rotationStrategy Rotation Strategy
   * @param compressionCodec a class encapsulates a streaming compression/decompression pair.
   * @param settings Hdfs writing settings
   */
  def compressed(
      fs: FileSystem,
      syncStrategy: SyncStrategy,
      rotationStrategy: RotationStrategy,
      compressionCodec: CompressionCodec,
      settings: HdfsWritingSettings
  ): Flow[ByteString, WriteLog, NotUsed] =
    Flow
      .fromGraph(
        new HdfsFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HdfsWriter.CompressedDataWriter(
            fs,
            compressionCodec,
            settings.outputFileGenerator,
            settings.overwrite
          )
        )
      )
      .mapAsync(1)(identity)

  /*
   * Scala API: creates a Flow with [[HdfsFlowStage]] for [[SequenceFile.Writer]]
   *
   * @param fs Hdfs FileSystem
   * @param syncStrategy sync strategy
   * @param rotationStrategy rotation strategy
   * @param compressionType a compression type used to compress key/value pairs in the SequenceFile
   * @param compressionCodec a class encapsulates a streaming compression/decompression pair.
   * @param settings Hdfs writing settings
   * @param classK a key class
   * @param classV a value class
   */
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
        new HdfsFlowStage(
          syncStrategy,
          rotationStrategy,
          settings,
          HdfsWriter.SequenceWriter(fs, compressionType, compressionCodec, classK, classV, settings.outputFileGenerator)
        )
      )
      .mapAsync(1)(identity)

}
