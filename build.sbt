name := "solution"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "2.0.2"
libraryDependencies += "joda-time"         % "joda-time"  % "2.9.1"

libraryDependencies += "org.scalatest"    %% "scalatest"  % "3.0.1" % Test

mainClass in assembly := Some("solution.Solution")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
