
lazy val commonSettings = Seq(
  scalaVersion := "2.11.8",
  version :="0.0.5",
  parallelExecution in Test := false
) ++ reformatOnCompileSettings


lazy val shared = project.in(file("shared"))
  .settings(
    name := "tasks-shared",
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "upickle" % "0.4.1"
    )
  )
  .settings(commonSettings:_*)

resolvers += Resolver.jcenterRepo


lazy val core = project.in(file("core"))
  .settings(commonSettings:_*)
  .settings(
    name := "tasks-core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.17",
      "com.typesafe.akka" %% "akka-remote" % "2.4.17",
      "com.typesafe.akka" %% "akka-testkit" % "2.4.17",
      "com.typesafe.akka" %% "akka-http-core" % "10.0.5",
      "com.typesafe" % "config" % "1.3.0",
      "io.github.pityka" %% "s3-stream-fork" % "0.0.4-fork4-SNAPSHOT",
      "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.24",
      "org.iq80.leveldb" % "leveldb" % "0.9",
      "ch.ethz.ganymed" % "ganymed-ssh2" % "261",
      "org.scalatest" %% "scalatest" % "2.1.5" % "test",
      "io.spray" %% "spray-routing" % "1.3.3",
      "io.spray" %% "spray-can" % "1.3.3",
      "com.lihaoyi" %% "upickle" % "0.4.3",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  )
  .dependsOn(shared)

lazy val example = project.in(file("example"))
.settings(commonSettings:_*)
.dependsOn(core)
.enablePlugins(JavaAppPackaging)
.settings(
    executableScriptName := "entrypoint",
    topLevelDirectory := None
)
