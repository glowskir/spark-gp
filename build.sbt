name := "spark-gp"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

enablePlugins(GitVersioning)