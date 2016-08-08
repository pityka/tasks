
lazy val commonSettings = Seq(
  scalaVersion := "2.11.8"
)


lazy val TasksMonitorWebShared = project.in(file("TasksMonitorWebShared"))
  .settings(
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "upickle" % "0.4.1"
    )
  )
  .settings(commonSettings:_*)

lazy val Tasks = project.in(file("Tasks"))
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-remote" % "2.3.15",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.15",
      "com.google.guava" % "guava" % "18.0",
      "com.google.code.findbugs" % "jsr305" % "1.3.9",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.24",
      "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.24",
      "org.iq80.leveldb" % "leveldb" % "0.9",
      "ch.ethz.ganymed" % "ganymed-ssh2" % "261",
      "us.levk" % "drmaa-common" % "1.0"


    )
  )
  .dependsOn(TasksMonitorWebShared)
