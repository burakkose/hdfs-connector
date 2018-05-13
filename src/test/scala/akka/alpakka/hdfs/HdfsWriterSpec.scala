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
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.test.PathUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

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

      val res = Await.result(resF, Duration.Inf)
      res shouldBe Seq(
        WriteLog("0", 0),
        WriteLog("1", 1),
        WriteLog("2", 2),
        WriteLog("3", 3),
        WriteLog("4", 4)
      )
      getNonZeroLengthFiles("/tmp/alpakka/").size shouldEqual res.size
      readFromLogs(res) shouldBe books.map(_.utf8String).toSeq
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

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 2

      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual res.size
      files.map(_.getLen).sum shouldEqual 1024
      files.foreach(f => f.getLen should be <= 512L)
      readFromLogs(res).flatten shouldBe data.flatMap(_.utf8String)
    }

    "detect upstream finish and move data to hdfs" in {
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

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 1

      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual res.size

      files.head.getLen shouldEqual 1024
      readFromLogs(res).flatten shouldBe data.flatMap(_.utf8String)
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

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 3
      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual res.size
      readFromLogs(res) shouldBe books.map(_.utf8String).grouped(2).map(_.mkString).toSeq
    }

    "use timed rotation" in {
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
      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual 3
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
      val files = getNonZeroLengthFiles("/tmp/alpakka/")
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

      val res = Await.result(resF, Duration.Inf)
      res shouldBe Seq(
        WriteLog("0", 0),
        WriteLog("1", 1),
        WriteLog("2", 2),
        WriteLog("3", 3)
      )

      getNonZeroLengthFiles("/tmp/alpakka/").size shouldEqual res.size
      readFromLogs(res) shouldBe messagesFromKafka.map(_.book.title)
    }
  }

  "CompressedDataWriter" should {
    "use file size rotation and produce six files" in {

      val codec = new DefaultCodec()
      codec.setConf(fs.getConf)

      fs.getConf.setEnum("zlib.compress.level", CompressionLevel.BEST_SPEED)

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

      val res = Await.result(resF, Duration.Inf)
      res shouldBe Seq(
        WriteLog("0.deflate", 0),
        WriteLog("1.deflate", 1),
        WriteLog("2.deflate", 2),
        WriteLog("3.deflate", 3),
        WriteLog("4.deflate", 4),
        WriteLog("5.deflate", 5)
      )

      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual res.size

      val pureContent: String = content.map(_.utf8String).mkString
      val contentFromHdfsWithCodec: String = readFromLogsWithCodec(res, codec).mkString
      val contentFromHdfs: String = readFromLogs(res).mkString
      contentFromHdfs should !==(pureContent)
      contentFromHdfsWithCodec shouldEqual pureContent
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

      val res = Await.result(resF, Duration.Inf)
      res shouldBe Seq(
        WriteLog("0.deflate", 0),
        WriteLog("1.deflate", 1),
        WriteLog("2.deflate", 2),
        WriteLog("3.deflate", 3),
        WriteLog("4.deflate", 4)
      )

      val files = getNonZeroLengthFiles("/tmp/alpakka/")
      files.size shouldEqual res.size

      val pureContent = books.map(_.utf8String).toSeq
      readFromLogs(res) should !==(pureContent) // content should be compressed
      readFromLogsWithCodec(res, codec) shouldEqual pureContent
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

  private def getNonZeroLengthFiles(path: String): Seq[FileStatus] = {
    val p = new Path(path)
    for (file <- fs.listStatus(p) if file.getLen > 0)
      yield file
  }

  override protected def afterEach(): Unit = {
    fs.delete(new Path("/tmp/alpakka/"), true)
    fs.delete(settings.pathGenerator(0, 0).getParent, true)
    ()
  }

  override protected def beforeAll(): Unit =
    setupCluster()

  override protected def afterAll(): Unit = {
    fs.close()
    hdfsCluster.shutdown()
  }

  private def readFromLogs(logs: Seq[WriteLog]): Seq[String] =
    logs.map(log => new Path(destionation, log.path)).map(readContent)

  private def readFromLogsWithCodec(logs: Seq[WriteLog], codec: CompressionCodec): Seq[String] =
    logs.map(log => new Path(destionation, log.path)).map(readContentWithCodec2(_, codec))

  private def readContent(file: Path): String =
    read(fs.open(file))

  private def readContentWithCodec2(file: Path, codec: CompressionCodec): String =
    read(codec.createInputStream(fs.open(file)))

  private def read(stream: InputStream): String = {
    val writer = new StringWriter
    IOUtils.copy(stream, writer, "UTF-8")
    writer.toString
  }

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
}
