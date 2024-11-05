import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / versionPolicyIntention := Compatibility.None
ThisBuild / versionPolicyIgnoredInternalDependencyVersions := Some(
  "^\\d+\\.\\d+\\.\\d+\\+\\d+".r
)

inThisBuild(
  List(
    organization := "io.github.pityka",
    homepage := Some(url("https://pityka.github.io/tasks/")),
    licenses := List(
      ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))
    ),
    developers := List(
      Developer(
        "pityka",
        "Istvan Bartha",
        "bartha.pityu@gmail.com",
        url("https://github.com/pityka/tasks")
      )
    )
  )
)

lazy val commonSettings = Seq(
  scalaVersion := "2.13.14",
  crossScalaVersions := Seq("2.13.14", "3.4.3"),
  parallelExecution in Test := false,
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 13)) =>
      Seq(
        "-deprecation", // Emit warning and location for usages of deprecated APIs.
        "-encoding",
        "utf-8", // Specify character encoding used by source files.
        "-feature", // Emit warning and location for usages of features that should be imported explicitly.
        "-language:postfixOps",
        "-language:implicitConversions",
        "-unchecked", // Enable additional warnings where generated code depends on assumptions.
        // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
        "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
        "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
        "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
        "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
        "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
        "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
        "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
        "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
        "-Xlint:option-implicit", // Option.apply used implicit view.
        "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
        "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
        "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
        "-Xlint:type-parameter-shadow" // A local type parameter shadows a type already in scope.
        // "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
        // "-Ywarn-numeric-widen", // Warn when numerics are widened.
        // "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
        // "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
        // "-Ywarn-unused:locals", // Warn if a local definition is unused.
        // "-Ywarn-unused:params", // Warn if a value parameter is unused.
        // "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
        // "-Ywarn-unused:privates" // Warn if a private member is unused.
      )
    case _ =>
      Seq(
        "-experimental",
        "-encoding",
        "utf-8",
        "-feature",
        "-language:postfixOps",
        "-language:implicitConversions",
        "-unchecked",
        "-deprecation"
      )
  })
) ++ Seq(
  fork := true,
  cancelable in Global := true,
  scalacOptions in (Compile, doc) ~= (_ filterNot (_ == "-Xfatal-warnings")),
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings"))
)

lazy val circeVersion = "0.14.9"
lazy val jsoniterVersion = "2.30.9"
lazy val akkaVersion = "2.6.19"
lazy val http4sVersion = "0.23.27"
lazy val scribeVersion = "3.13.3"
lazy val fs2Version = "3.11.0"

lazy val shared = crossProject(JSPlatform, JVMPlatform)
  .crossType(sbtcrossproject.CrossPlugin.autoImport.CrossType.Pure)
  .in(file("shared"))
  .settings(
    name := "tasks-shared",
    libraryDependencies ++= Seq(
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % jsoniterVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal"
    )
  )
  .settings(commonSettings: _*)
  .jsSettings(
    fork := false,
    libraryDependencies ++= Seq(
      "com.github.plokhotnyuk.jsoniter-scala" %%% "jsoniter-scala-core" % jsoniterVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %%% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal"
    )
  )

lazy val sharedJVM = shared.jvm

lazy val sharedJS = shared.js

resolvers += Resolver.jcenterRepo

lazy val spores = project
  .in(file("spores"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-spores",
    libraryDependencies ++= Seq(
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % jsoniterVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal"
    ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) =>
        Seq(
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      case _ => Seq.empty
    }) ++ List(
      "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    )
  )
lazy val akkaProvided = List(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Provided,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Provided,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion % Provided
)
lazy val akkaReal = List(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion ,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion ,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion 
)
lazy val core = project
  .in(file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-core",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-io" % fs2Version,
      "co.fs2" %% "fs2-reactive-streams" % fs2Version,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "software.amazon.awssdk" % "s3" % "2.23.13",
      "com.google.guava" % "guava" % "33.0.0-jre", // scala-steward:off
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "com.typesafe" % "config" % "1.4.2",
      "org.typelevel" %% "cats-effect" % "3.5.3",
      "io.github.pityka" %% "selfpackage" % "2.1.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "com.outr" %% "scribe" % scribeVersion,
      "com.outr" %% "scribe-slf4j" % scribeVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "test"
    ) ++ akkaProvided ++ (CrossVersion
      .partialVersion(scalaVersion.value) match {
      case Some((2, 13)) =>
        Seq(
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
      case _ => Seq.empty
    })
  )
  .dependsOn(sharedJVM, spores)

lazy val ec2 = project
  .in(file("ec2"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ec2",
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-ec2" % "1.12.244" // scala-steward:off
    ) ++ akkaProvided
  )
  .dependsOn(core)

lazy val ssh = project
  .in(file("ssh"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ssh",
    libraryDependencies ++= Seq(
      "ch.ethz.ganymed" % "ganymed-ssh2" % "262"
    ) ++ akkaProvided
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val kubernetes = project
  .in(file("kubernetes"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-kubernetes",
    libraryDependencies ++= Seq(
      "com.goyeau" %% "kubernetes-client" % "0.11.0",
      "io.github.pityka" %% "selfpackage-jib" % "2.1.3",
      
    ) ++ akkaProvided
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val kubernetesTest = project 
 .in(file("kubernetes-test"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-kubernetes-test",
    publish / skip := true,
    libraryDependencies ++= akkaReal
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(kubernetes)

// lazy val tracker = project
//   .in(file("tracker"))
//   .settings(commonSettings: _*)
//   .settings(
//     name := "tasks-tracker",
//     libraryDependencies ++= Seq(
//       "org.scalatest" %% "scalatest" % "3.2.10" % "test",
//       "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal"
//     ) ++ akkaProvided,
//     // resources in Compile += (fastOptJS in Compile in uifrontend).value.data
//   )
//   .dependsOn(core % "compile->compile;test->test")

// lazy val uibackend = project
//   .in(file("uibackend"))
//   .settings(commonSettings: _*)
//   .settings(
//     name := "tasks-ui-backend",
//     libraryDependencies ++= Seq(
//       "org.scalatest" %% "scalatest" % "3.2.10" % "test"
//     ) ++ akkaProvided,
//     resources in Compile += (fastOptJS in Compile in uifrontend).value.data
//   )
//   .dependsOn(core % "compile->compile;test->test")

// lazy val uifrontend = project
//   .in(file("uifrontend"))
//   .settings(commonSettings: _*)
//   .settings(
//     name := "tasks-ui-frontend",
//     libraryDependencies ++= Seq(
//       "org.scala-js" %%% "scalajs-dom" % "1.2.0",
//       "com.github.plokhotnyuk.jsoniter-scala" %%% "jsoniter-scala-core" % jsoniterVersion,
//       "com.github.plokhotnyuk.jsoniter-scala" %%% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal",
//       "com.raquo" %%% "laminar" % "0.13.0"
//     ),
//     mimaPreviousArtifacts := Set.empty,
//     fork := false
//   )
//   .dependsOn(sharedJS)
//   .enablePlugins(ScalaJSPlugin)

lazy val example = project
  .in(file("example"))
  .settings(commonSettings: _*)
  .dependsOn(core, ssh)
  .enablePlugins(JavaAppPackaging)
  .settings(
    executableScriptName := "entrypoint",
    topLevelDirectory := None,
    publishArtifact := false,
    publish / skip := true,
    libraryDependencies ++= Seq(
      ("com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal")
    ) ++ Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
    )
  )

lazy val upicklesupport = project
  .in(file("upickle"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-upickle",
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "upickle" % "1.4.4"
    ) ++ akkaProvided
  )
  .dependsOn(core)

lazy val circe = project
  .in(file("circe"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-circe",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    ) ++ akkaProvided
  )
  .dependsOn(core)

lazy val ecoll = project
  .in(file("collection"))
  .settings(commonSettings: _*)
  .settings(
    name := "tasks-ecoll",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "flatjoin-akka-stream" % "0.0.17",
      "io.github.pityka" %% "lame-bgzip-index" % "0.0.4",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % jsoniterVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "compile-internal",
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % jsoniterVersion % "test",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    ) ++ akkaProvided
  )
  .dependsOn(core)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    publish / skip := true,
    crossScalaVersions := Nil
  )
  .aggregate(
    spores,
    core,
    upicklesupport,
    circe,
    sharedJVM,
    sharedJS,
    ec2,
    ssh,
    // uibackend,
    // uifrontend,
    kubernetes,
    // tracker,
    example
  )

lazy val testables = (project in file("testables"))
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    publish / skip := true
  )
  .aggregate(
    spores,
    core,
    upicklesupport,
    circe,
    sharedJVM
    // ssh
  )
