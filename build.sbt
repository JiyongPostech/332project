ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "332project" 
  )

// --- Dependencies ---
libraryDependencies ++= Seq(
  // Netty (네트워크 계층 구현)
  "io.netty" % "netty-all" % "4.1.100.Final"
)
