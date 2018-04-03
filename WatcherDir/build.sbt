name := "WatcherDir"

version := "1.0"

scalaVersion := "2.12.2"

resolvers += "vertica" at "http://clojars.org/repo/"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2",
  "org.clojars.prepor" % "vertica-jdbc" % "7.0.1-0"	
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard; case _ => MergeStrategy.first
  }
}
