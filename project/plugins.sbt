addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.4")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.5"

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.7.0")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.0.0")

addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.0.0")
