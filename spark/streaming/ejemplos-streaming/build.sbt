name := "utad-streaming"

version := "0.0.1"
  
scalaVersion := "2.10.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1", 
  "org.apache.spark" %% "spark-mllib" % "1.3.1", 
  "org.apache.spark" %% "spark-streaming" % "1.3.1", 
  "org.apache.hadoop" % "hadoop-common" % "2.6.0"
  
)

