addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.9")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.5")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.10"

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.7.1")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.0.0")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")

addSbtPlugin("ch.epfl.scala" % "sbt-version-policy" % "2.0.1")
