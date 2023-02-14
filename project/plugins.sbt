addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.15")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.5")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.12"

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.10.0")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.11")

addSbtPlugin("ch.epfl.scala" % "sbt-version-policy" % "2.1.0")
