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

  private def books =
    List(
      "Akka Concurrency",
      "Akka in Action",
      "Effective Akka",
      "Learning Scala",
      "Programming in Scala",
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

  var fs: FileSystem = FileSystem.get(conf)
  //#init-client

  "Hdfs Data Writer" should {
    "use file size rotation and create five files" in {
      val f1 = HdfsFlow.data(
        fs,
        "myfiles",
        SyncStrategy.count(1),
        RotationStrategy.sized(0.01, FileUnit.KB),
        (rc, t) => new Path(s"/test/$rc.file"),
        HdfsWritingSettings()
      )

      val resF1 = Source
        .fromIterator(() => books)
        .via(f1)
        .runWith(Sink.seq)

      val res1 = Await.result(resF1, Duration.Inf)
      res1 shouldBe Seq(
        WriteLog("1.file", 1),
        WriteLog("2.file", 2),
        WriteLog("3.file", 3),
        WriteLog("4.file", 4),
        WriteLog("5.file", 5)
      )
      getNonZeroLengthFiles("myfiles").size shouldEqual 5
    }

    "use file size rotation and create exactly two files" in {
      val fakeData = ByteBuffer.allocate(1024).array().toIterator.map(ByteString(_)) // 1024 byte string (1kb)

      val flow = HdfsFlow.data(
        fs,
        "myfiles",
        SyncStrategy.count(500),
        RotationStrategy.sized(0.5, FileUnit.KB),
        (rc, _) => new Path(s"/test/$rc.file"),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => fakeData)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 2 // it should create 2 files
      val files = getNonZeroLengthFiles("myfiles")
      files.size shouldEqual 2
      files.foldLeft(0L) { case (acc, file) => acc + file.getLen } shouldEqual 1024
      files.foreach(f => f.getLen should be <= 512L)
    }

    "detect upstream finish and move data to hdfs" in {
      val fakeData = ByteBuffer.allocate(1024).array().toIterator.map(ByteString(_)) // 1024 byte string (1kb)

      val flow = HdfsFlow.data(
        fs,
        "myfiles",
        SyncStrategy.count(500),
        RotationStrategy.sized(1, FileUnit.GB), // Use huge rotation
        (rc, _) => new Path(s"/test/$rc.file"),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => fakeData)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 1
      val files = getNonZeroLengthFiles("myfiles")
      files.size shouldEqual 1
      files.head.getLen shouldEqual 1024
    }

    "use buffer rotation and create 3 files" in {
      val flow = HdfsFlow.data(
        fs,
        "myfiles",
        SyncStrategy.count(1),
        RotationStrategy.buffered(2),
        (rc, _) => new Path(s"/test/$rc.file"),
        HdfsWritingSettings()
      )

      val resF = Source
        .fromIterator(() => books)
        .via(flow)
        .runWith(Sink.seq)

      val res = Await.result(resF, Duration.Inf)
      res.size shouldEqual 3
      val files = getNonZeroLengthFiles("myfiles")
      files.size shouldEqual 3
    }

    "use timed rotation" in {
      val probe = TestProbe()

      val cancellable = Source
        .tick(0.microsecond, 50.milliseconds, ByteString("I love Alpakka!"))
        .via(
          HdfsFlow.data(
            fs,
            "myfiles",
            SyncStrategy.count(1),
            RotationStrategy.timed(500.milliseconds),
            (rc, _) => new Path(s"/test/$rc.file"),
            HdfsWritingSettings()
          )
        )
        .to(Sink.actorRef(probe.ref, "completed"))
        .run()

      probe.expectMsg(500.milliseconds, WriteLog("1.file", 1))
      probe.expectMsg(1000.milliseconds, WriteLog("2.file", 2))
      probe.expectMsg(1500.milliseconds, WriteLog("3.file", 3))
      cancellable.cancel()
      val files = getNonZeroLengthFiles("myfiles")
      files.size shouldEqual 3
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
    fs.delete(new Path("myfiles"), true)
    fs.delete(new Path("test"), true)
    ()
  }

  override protected def beforeAll(): Unit =
    setupCluster()

  override protected def afterAll(): Unit = {
    fs.close()
    hdfsCluster.shutdown()
  }
}
