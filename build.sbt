name := "spark-gp"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.2"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

enablePlugins(GitVersioning)

triggeredMessage in ThisBuild := Watched.clearWhenTriggered

cancelable in Global := true

scalacOptions += "-target:jvm-1.7"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)