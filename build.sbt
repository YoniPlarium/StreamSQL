name := "StreamSQL"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.5.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.5.0-cdh5.5.2"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"


resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "spray repo" at "http://repo.spray.io",
  "spray nightlies" at "http://nightlies.spray.io",
  "Spray Repository" at "http://repo.spray.cc/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "SnowPlow Repo" at "http://maven.snplow.com/releases/",
  Resolver.sonatypeRepo("public")
)
    