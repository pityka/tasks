import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

lazy val commonSettings = Seq(
  scalaVersion := "2.12.6",
  version := "0.0.35",
  parallelExecution in Test := false,
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:postfixOps",
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match", // Pattern match may not be typesafe.
    "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ypartial-unification", // Enable partial unification in type constructor inference
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
    "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates" // Warn if a private member is unused.
  )
) ++ Seq(
  organization := "io.github.pityka",
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  publishTo := sonatypePublishTo.value,
  pomExtra in Global := {
    <url>https://pityka.github.io/tasks/</url>
      <scm>
        <connection>scm:git:github.com/pityka/tasks</connection>
        <developerConnection>scm:git:git@github.com:pityka/tasks</developerConnection>
        <url>github.com/pityka/tasks</url>
      </scm>
      <developers>
        <developer>
          <id>pityka</id>
          <name>Istvan Bartha</name>
          <url>https://pityka.github.io/tasks/</url>
        </developer>
      </developers>
  },
  fork := true,
  cancelable in Global := true
)

lazy val shared = crossProject(JSPlatform, JVMPlatform)
  .crossType(sbtcrossproject.CrossPlugin.autoImport.CrossType.Pure)
  .in(file("shared"))
  .settings(
    name := "tasks-shared",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.10.1",
      "io.circe" %% "circe-generic" % "0.10.1",
      "io.circe" %% "circe-parser" % "0.10.1"
    )
  )
  .settings(commonSettings: _*)
  .jsSettings(fork := false)

lazy val sharedJVM = shared.jvm

lazy val sharedJS = shared.js

resolvers += Resolver.jcenterRepo

lazy val core = project
  .in(file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-core",
    resolvers += Resolver.bintrayRepo("beyondthelines", "maven"),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "22.0",
      "com.typesafe.akka" %% "akka-actor" % "2.5.18",
      "com.typesafe.akka" %% "akka-remote" % "2.5.18",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.18" % "test",
      "com.typesafe.akka" %% "akka-http-core" % "10.1.1",
      "com.typesafe" % "config" % "1.3.3",
      "io.github.pityka" %% "selfpackage" % "1.2.3",
      "io.github.pityka" %% "s3-stream-fork" % "0.0.3",
      "io.github.pityka" %% "akka-http-unboundedqueue" % "1.2.0",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )
  )
  .dependsOn(sharedJVM)

lazy val ec2 = project
  .in(file("ec2"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ec2",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.24"
    )
  )
  .dependsOn(core)

lazy val ssh = project
  .in(file("ssh"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ssh",
    libraryDependencies ++= Seq(
      "ch.ethz.ganymed" % "ganymed-ssh2" % "261"
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val kubernetes = project
  .in(file("kubernetes"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-kubernetes",
    libraryDependencies ++= Seq(
      "io.fabric8" % "kubernetes-client" % "4.0.4"
    )
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val tracker = project
  .in(file("tracker"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-tracker",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test"),
    resources in Compile += (fastOptJS in Compile in uifrontend).value.data
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val uibackend = project
  .in(file("uibackend"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ui-backend",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.0" % "test"),
    resources in Compile += (fastOptJS in Compile in uifrontend).value.data
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val uifrontend = project
  .in(file("uifrontend"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ui-frontend",
    libraryDependencies ++= Seq(
      "com.lihaoyi" %%% "scalatags" % "0.6.7",
      "org.scala-js" %%% "scalajs-dom" % "0.9.5",
      "io.circe" %%% "circe-core" % "0.10.1",
      "io.circe" %%% "circe-parser" % "0.10.1",
      "io.circe" %%% "circe-generic" % "0.10.1",
      "net.pishen" %%% "akka-ui" % "0.1.1"
    ),
    fork := false
  )
  .dependsOn(sharedJS)
  .enablePlugins(ScalaJSPlugin)

lazy val example = project
  .in(file("example"))
  .settings(commonSettings: _*)
  .dependsOn(core, collection, ssh)
  .enablePlugins(JavaAppPackaging)
  .settings(
    executableScriptName := "entrypoint",
    topLevelDirectory := None
  )

lazy val upicklesupport = project
  .in(file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-upickle",
    libraryDependencies += "com.lihaoyi" %% "upickle" % "0.4.4"
  )
  .dependsOn(core)

lazy val jsoniter = project
  .in(file("jsoniter"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-jsoniter",
    libraryDependencies +=
      "com.github.plokhotnyuk.jsoniter-scala" %% "macros" % "0.23.0"
  )
  .dependsOn(core)

lazy val collection = project
  .in(file("collection"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-collection",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "flatjoin-akka-stream" % "0.0.12",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test")
  )
  .dependsOn(core)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false
  )
  .aggregate(core,
             collection,
             upicklesupport,
             jsoniter,
             sharedJVM,
             sharedJS,
             ec2,
             ssh,
             uibackend,
             uifrontend,
             kubernetes,
             tracker,
             example)

scalafmtOnCompile in ThisBuild := true

parallelExecution in ThisBuild := false
