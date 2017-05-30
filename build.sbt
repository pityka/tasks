
lazy val commonSettings = Seq(
  scalaVersion := "2.12.2",
  version :="0.0.9-SNAPSHOT",
  parallelExecution in Test := false
  // scalacOptions ++= Seq("-Xlog-implicits")
) //++ reformatOnCompileSettings


lazy val shared = project.in(file("shared"))
  .settings(
    name := "tasks-shared",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.8.0",
      "io.circe" %% "circe-generic" % "0.8.0",
      "io.circe" %% "circe-parser" % "0.8.0"
    )
  ).enablePlugins(ScalafmtPlugin)
  .settings(commonSettings:_*)

resolvers += Resolver.jcenterRepo

lazy val core = project.in(file("core"))
  .settings(commonSettings:_*)
  .settings(
    name := "tasks-core",
    resolvers +=  Resolver.bintrayRepo("beyondthelines", "maven"),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "22.0",
      "com.typesafe.akka" %% "akka-actor" % "2.4.17",
      "com.typesafe.akka" %% "akka-remote" % "2.4.17",
      "com.typesafe.akka" %% "akka-testkit" % "2.4.17",
      "com.typesafe.akka" %% "akka-http-core" % "10.0.5",
      "com.typesafe" % "config" % "1.3.0",
      "io.github.pityka" %% "akka-http-unboundedqueue" % "1.0.0",
      "io.github.pityka" %% "selfpackage" % "0.0.1",
      "io.github.pityka" %% "s3-stream-fork" % "0.0.3-SNAPSHOT",
      "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.24",
      "ch.ethz.ganymed" % "ganymed-ssh2" % "261",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  ).enablePlugins(ScalafmtPlugin)
  .dependsOn(shared)

lazy val example = project.in(file("example"))
.settings(commonSettings:_*)
.dependsOn(core,collection)
.enablePlugins(JavaAppPackaging)
.settings(
    executableScriptName := "entrypoint",
    topLevelDirectory := None
)

lazy val upicklesupport = project.in(file("upickle")).settings(commonSettings:_*).settings(
  name := "tasks-upickle",
  libraryDependencies += "com.lihaoyi" %% "upickle" % "0.4.4"
).dependsOn(core)

lazy val collection = project.in(file("collection")).settings(commonSettings:_*)
.settings(
  name := "tasks-collection",
  libraryDependencies ++= Seq("io.github.pityka" %% "flatjoin-akka-stream" % "0.0.1-SNAPSHOT",
    "io.github.pityka" %% "flatjoin-upickle" % "0.0.1-SNAPSHOT")
).dependsOn(core).enablePlugins(ScalafmtPlugin)
