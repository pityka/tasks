addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.4")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.3")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.15")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.0"

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "0.6.32")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "0.6.0")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.4.31")
