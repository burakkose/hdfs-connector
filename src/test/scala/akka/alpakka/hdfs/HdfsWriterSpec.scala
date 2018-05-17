package akka.alpakka.hdfs

import java.io.{File, InputStream, StringWriter}
import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hdfs.scaladsl.{FileUnit, HdfsFlow, RotationStrategy, SyncStrategy}
import akka.stream.alpakka.hdfs.{HdfsWritingSettings, WriteLog}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import akka.util.ByteString
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.{SequenceFile, Text, Writable}
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.test.PathUtils
import org.apache.hadoop.util.ReflectionUtils
import org.scalatest.{Assertion, _}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}
import scala.util.Random

class HdfsWriterSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var hdfsCluster: MiniDFSCluster = _
  private val destionation = "/tmp/alpakka/"

  private def books: Iterator[ByteString] =
    List(
      "Akka Concurrency",
      "Akka in Action",
      "Effective Akka",
      "Learning Scala",
      "Programming in Scala Programming",
    ).map(ByteString(_)).iterator

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.FileSystem

  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:54310")
  // conf.setEnum("zlib.compress.level", CompressionLevel.BEST_COMPRESSION)

  var fs: FileSystem = FileSystem.get(conf)
  //#init-client

  fs.getConf.setEnum("zlib.compress.level", CompressionLevel.BEST_SPEED)

  val settings = HdfsWritingSettings()

  "DataWriter" should {
    "use file size rotation and produce five files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(50),
        RotationStrategy.sized(0.01, FileUnit.KB),
        settings
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        WriteLog("0", 0),
        WriteLog("1", 1),
        WriteLog("2", 2),
        WriteLog("3", 3),
        WriteLog("4", 4)
      )

      verifyOutputFileSize(logs)
      readLogs(logs) shouldBe books.map(_.utf8String).toSeq
    }

    "use file size rotation and produce exactly two files" in {
      val data = generateFakeContent(1, FileUnit.KB.byteCount)
      val dataIterator = data.toIterator

      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(500),
        RotationStrategy.sized(0.5, FileUnit.KB),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => dataIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 2

      val files = getNonZeroLengthFiles
      files.map(_.getLen).sum shouldEqual 1024
      files.foreach(f => f.getLen should be <= 512L)

      verifyOutputFileSize(logs)
      readLogs(logs).flatten shouldBe data.flatMap(_.utf8String)
    }

    "detect upstream finish and move remaining data to hdfs" in {
      val data = generateFakeContent(1, FileUnit.KB.byteCount)
      val dataIterator = data.toIterator

      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(500),
        RotationStrategy.sized(1, FileUnit.GB), // Use huge rotation
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => dataIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 1

      getNonZeroLengthFiles.head.getLen shouldEqual 1024

      verifyOutputFileSize(logs)
      readLogs(logs).flatten shouldBe data.flatMap(_.utf8String)
    }

    "use buffer rotation and produce three files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(1),
        RotationStrategy.counted(2),
        settings
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs.size shouldEqual 3

      verifyOutputFileSize(logs)
      readLogs(logs) shouldBe books.map(_.utf8String).grouped(2).map(_.mkString).toSeq
    }

    "use timed rotation" in {
      //todo this is broken
      val probe = TestProbe()

      val cancellable = Source
        .tick(0.millis, 50.milliseconds, ByteString("I love Alpakka!"))
        .via(
          HdfsFlow.data(
            fs,
            SyncStrategy.count(100),
            RotationStrategy.timed(500.milliseconds),
            HdfsWritingSettings()
          )
        )
        .to(Sink.actorRef(probe.ref, "completed"))
        .run()

      probe.expectMsg(500.milliseconds, WriteLog("0", 0))
      probe.expectMsg(1000.milliseconds, WriteLog("1", 1))
      probe.expectMsg(1500.milliseconds, WriteLog("2", 2))
      cancellable.cancel()
      probe.expectMsg(2000.milliseconds, WriteLog("3", 3))
      getNonZeroLengthFiles.size shouldEqual 3
    }

    "should use no rotation and produce one file" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.none,
        RotationStrategy.none,
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 1

      val files = getNonZeroLengthFiles
      files.size shouldEqual res.size
      files.head.getLen shouldEqual books.map(_.toArray.length).sum
    }

    /*    "use timed rotation ang ignore empty files" in {
      val probe = TestProbe()

      val cancellable = Source
        .tick(0.millis, 1000.millis, ByteString("I love Alpakka!"))
        .via(
          HdfsFlow.data(
            fs,
            SyncStrategy.count(100),
            RotationStrategy.timed(100.millis),
            HdfsWritingSettings()
          )
        )
        .to(Sink.actorRef(probe.ref, "completed"))
        .run()

      probe.expectMsg(100.millis, WriteLog("0", 0))
      probe.expectNoMessage(500.millis)
      cancellable.cancel()
      val files = getNonZeroLengthFiles(settings.destination)
      println(files.toList)
      files.size shouldEqual 1
    }*/

    "kafka-example - store data" in {
      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to HDFS, we want
      // to commit the offset to Kafka
      case class Book(title: String)
      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Akka Concurrency"), KafkaOffset(0)),
        KafkaMessage(Book("Akka in Action"), KafkaOffset(1)),
        KafkaMessage(Book("Effective Akka"), KafkaOffset(2)),
        KafkaMessage(Book("Learning Scala"), KafkaOffset(3))
      )

      var committedOffsets = List[KafkaOffset]()

      def commitToKakfa(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(50),
        RotationStrategy.sized(0.01, FileUnit.KB),
        settings
      )

      val resF = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          // Transform message so that we can write to hdfs
          ByteString(book.title)
        }
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        WriteLog("0", 0),
        WriteLog("1", 1),
        WriteLog("2", 2),
        WriteLog("3", 3)
      )

      verifyOutputFileSize(logs)
      readLogs(logs) shouldBe messagesFromKafka.map(_.book.title)
    }
  }

  "CompressedDataWriter" should {
    "use file size rotation and produce six files" in {

      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategy.count(1),
        RotationStrategy.sized(0.1, FileUnit.MB),
        codec,
        settings
      )

      val content = generateFakeContentWithPartitions(1, FileUnit.MB.byteCount, 30)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        WriteLog("0.deflate", 0),
        WriteLog("1.deflate", 1),
        WriteLog("2.deflate", 2),
        WriteLog("3.deflate", 3),
        WriteLog("4.deflate", 4),
        WriteLog("5.deflate", 5)
      )

      verifyOutputFileSize(logs)
      verifyLogsWithCodec(content, logs, codec)
    }

    "use buffer rotation and produce five files" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategy.count(1),
        RotationStrategy.counted(1),
        codec,
        settings
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldBe Seq(
        WriteLog("0.deflate", 0),
        WriteLog("1.deflate", 1),
        WriteLog("2.deflate", 2),
        WriteLog("3.deflate", 3),
        WriteLog("4.deflate", 4)
      )

      verifyOutputFileSize(logs)
      verifyLogsWithCodec(books.toSeq, logs, codec)
    }

    "should use no rotation and produce one file" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.compressed(
        fs,
        SyncStrategy.none,
        RotationStrategy.none,
        codec,
        settings
      )

      val content = generateFakeContentWithPartitions(1, FileUnit.MB.byteCount, 30)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)
      logs shouldEqual Seq(
        WriteLog("0.deflate", 0)
      )

      verifyOutputFileSize(logs)
      verifyLogsWithCodec(content, logs, codec)
    }
  }

  "SequenceWriter" should {
    "use file size rotation and produce six files without a compression" in {
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategy.none,
        RotationStrategy.sized(1, FileUnit.MB),
        settings,
        classOf[Text],
        classOf[Text]
      )

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      verifyOutputFileSize(logs)
      verifySequenceFile(content, logs)
    }

    "use file size rotation and produce six files with a compression" in {
      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategy.none,
        RotationStrategy.sized(1, FileUnit.MB),
        CompressionType.BLOCK,
        codec,
        settings,
        classOf[Text],
        classOf[Text]
      )

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      verifyOutputFileSize(logs)
      verifySequenceFile(content, logs)
    }

    "use buffer rotation and produce five files" in {
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategy.none,
        RotationStrategy.counted(1),
        settings,
        classOf[Text],
        classOf[Text]
      )

      def content = books.zipWithIndex.map {
        case (data, index) =>
          new Text(index.toString) -> new Text(data.utf8String)
      }

      val resF = Source
        .fromIterator(() => content)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      logs.size shouldEqual 5
      verifyOutputFileSize(logs)
      verifySequenceFile(content.toSeq, logs)
    }

    "should use no rotation and produce one file" in {
      val flow = HdfsFlow.sequence(
        fs,
        SyncStrategy.none,
        RotationStrategy.none,
        settings,
        classOf[Text],
        classOf[Text]
      )

      // half MB data becomes more when it is sequence
      val content = generateFakeContentForSequence(0.5, FileUnit.MB.byteCount)

      val resF = Source
        .fromIterator(() => content.toIterator)
        .via(flow)
        .runWith(Sink.seq)

      val logs = Await.result(resF, Duration.Inf)

      logs.size shouldEqual 1
      verifyOutputFileSize(logs)
      verifySequenceFile(content, logs)
    }
  }

  private def setupCluster(): Unit = {
    val baseDir = new File(PathUtils.getTestDir(getClass), "miniHDFS")
    val conf = new HdfsConfiguration
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(54310).format(true).build()
    hdfsCluster.waitClusterUp()
  }

  private def getNonZeroLengthFiles: Seq[FileStatus] = {
    val p = new Path(destionation)
    for (file <- fs.listStatus(p) if file.getLen > 0)
      yield file
  }

  override protected def afterEach(): Unit = {
    fs.delete(new Path(destionation), true)
    fs.delete(settings.pathGenerator(0, 0).getParent, true)
    ()
  }

  override protected def beforeAll(): Unit =
    setupCluster()

  override protected def afterAll(): Unit = {
    fs.close()
    hdfsCluster.shutdown()
  }

  private def readLogs(logs: Seq[WriteLog]): Seq[String] =
    logs.map(log => new Path(destionation, log.path)).map(f => read(fs.open(f)))

  private def readLogsWithCodec(logs: Seq[WriteLog], codec: CompressionCodec): Seq[String] =
    logs.map(log => new Path(destionation, log.path)).map { file =>
      read(codec.createInputStream(fs.open(file)))
    }

  private def read(stream: InputStream): String = {
    val writer = new StringWriter
    IOUtils.copy(stream, writer, "UTF-8")
    writer.toString
  }

  private def verifyOutputFileSize(logs: Seq[WriteLog]): Assertion =
    getNonZeroLengthFiles.size shouldEqual logs.size

  private def verifyLogsWithCodec(content: Seq[ByteString], logs: Seq[WriteLog], codec: CompressionCodec): Assertion = {
    val pureContent: String = content.map(_.utf8String).mkString
    val contentFromHdfsWithCodec: String = readLogsWithCodec(logs, codec).mkString
    val contentFromHdfs: String = readLogs(logs).mkString
    contentFromHdfs should !==(pureContent)
    contentFromHdfsWithCodec shouldEqual pureContent
  }

  def readSequenceFile(log: WriteLog): List[(Text, Text)] = {
    val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(destionation, log.path)))
    var key = new Text
    var value = new Text
    val results = new ListBuffer[(Text, Text)]()
    while (reader.next(key, value)) {
      results += ((key, value))
      key = new Text
      value = new Text
    }
    results.toList
  }

  private def verifySequenceFile(content: Seq[(Text, Text)], logs: Seq[WriteLog]): Assertion =
    logs.flatMap(readSequenceFile) shouldEqual content

  /*
    It generates count * byte size list with random char
   */
  private def generateFakeContent(count: Double, byte: Long): List[ByteString] =
    ByteBuffer
      .allocate((count * byte).toInt)
      .array()
      .toList
      .map(_ => Random.nextPrintableChar)
      .map(ByteString(_))

  private def generateFakeContentWithPartitions(count: Double, byte: Long, partition: Int): List[ByteString] = {
    val fakeData = generateFakeContent(count, byte)
    val groupSize = Math.ceil(fakeData.size / partition.toDouble).toInt
    fakeData.grouped(groupSize).map(list => ByteString(list.map(_.utf8String).mkString)).toList
  }

  private def generateFakeContentForSequence(count: Double, byte: Long): List[(Text, Text)] = {
    val half = ((count * byte) / 2).toInt
    (0 to half)
      .map(_ => (new Text(Random.nextPrintableChar.toString), new Text(Random.nextPrintableChar.toString)))
      .toList
  }
}
