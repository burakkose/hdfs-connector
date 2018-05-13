name := "hdfs-connector"

organization := "burakkose"

version := "0.1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= {
  val akkaVersion = "2.5.9"
  val scalaTestVersion = "3.0.4"
  val junitInterfaceVersion = "0.11"
  val hadoopVersion = "3.1.0"
  val catsVersion = "1.0.1"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.typelevel" %% "cats-core" % catsVersion,
    //Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
    "com.novocode" % "junit-interface" % junitInterfaceVersion % Test,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test
  )
}

parallelExecution in ThisBuild := false
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a")
