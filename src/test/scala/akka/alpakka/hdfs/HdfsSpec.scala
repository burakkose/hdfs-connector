package akka.alpakka.hdfs

import java.io.File
import java.nio.ByteBuffer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.hdfs.scaladsl.{FileUnit, HdfsFlow, RotationStrategy, SyncStrategy}
import akka.stream.alpakka.hdfs.{HdfsWritingSettings, WriteLog}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import akka.util.ByteString
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.test.PathUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

class HdfsSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var hdfsCluster: MiniDFSCluster = _

  private def books: Iterator[ByteString] =
    List(
      "Akka Concurrency",
      "Akka in Action",
      "Effective Akka",
      "Learning Scala",
      "Programming in Scala",
    ).map(ByteString(_)).iterator

  private def fakeData: Iterator[ByteString] =
    ByteBuffer.allocate(1024).array().toIterator.map(ByteString(_)) // 1024 byte string (1kb)

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.FileSystem

  val conf = new Configuration()
  conf.set("fs.default.name", "hdfs://localhost:54310")

  var fs: FileSystem = FileSystem.get(conf)
  //#init-client

  val settings = HdfsWritingSettings()

  "DataWriter" should {

    "use file size rotation and produce five files" in {
      val f1 = HdfsFlow.data(
        fs,
        SyncStrategy.count(1),
        RotationStrategy.sized(0.01, FileUnit.KB),
        settings
      )

      val resF1 = Source
        .fromIterator(() => books)
        .via(f1)
        .runWith(Sink.seq)

      val res1 = Await.result(resF1, Duration.Inf)
      res1 shouldBe Seq(
        WriteLog("0", 0),
        WriteLog("1", 1),
        WriteLog("2", 2),
        WriteLog("3", 3),
        WriteLog("4", 4)
      )
      getNonZeroLengthFiles(settings.destination).size shouldEqual 5
    }

    "use file size rotation and produce exactly two files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(500),
        RotationStrategy.sized(0.5, FileUnit.KB),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => fakeData)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 2 // it should create 2 files
      val files = getNonZeroLengthFiles(settings.destination)
      files.size shouldEqual 2
      files.foldLeft(0L) { case (acc, file) => acc + file.getLen } shouldEqual 1024
      files.foreach(f => f.getLen should be <= 512L)
    }

    "detect upstream finish and move data to hdfs" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(500),
        RotationStrategy.sized(1, FileUnit.GB), // Use huge rotation
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => fakeData)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 1
      val files = getNonZeroLengthFiles(settings.destination)
      files.size shouldEqual 1
      files.head.getLen shouldEqual 1024
    }

    "use buffer rotation and produce 3 files" in {
      val flow = HdfsFlow.data(
        fs,
        SyncStrategy.count(1),
        RotationStrategy.counted(2),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 3
      val files = getNonZeroLengthFiles(settings.destination)
      files.size shouldEqual 3
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
      val files = getNonZeroLengthFiles(settings.destination)
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
      val files = getNonZeroLengthFiles(settings.destination)
      files.size shouldEqual 1
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
    fs.delete(new Path(settings.destination), true)
    fs.delete(settings.outputFileGenerator(0, 0).getParent, true)
    ()
  }

  override protected def beforeAll(): Unit =
    setupCluster()

  override protected def afterAll(): Unit = {
    fs.close()
    hdfsCluster.shutdown()
  }
}
