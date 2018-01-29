name := "hdfs-connector"

organization := "burakkose"

version := "0.1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= {
  val akkaVersion = "2.5.9"
  val scalaTestVersion = "3.0.4"
  val junitInterfaceVersion = "0.11"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    //Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
    "com.novocode" % "junit-interface" % junitInterfaceVersion % Test
  )
}

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a")
