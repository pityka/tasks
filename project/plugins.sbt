addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.3")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.2")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.10"

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.5.1")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.0.0")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")
