ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "332project" 
  )

// .proto → Scala 코드 생성 (gRPC 포함)
Compile / PB.targets := Seq(
  scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
)
// --- Dependencies ---
libraryDependencies ++= Seq(
  // gRPC / Protobuf (Java)
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "io.grpc" % "grpc-stub"  % scalapb.compiler.Version.grpcJavaVersion,
  "com.google.protobuf" % "protobuf-java" % scalapb.compiler.Version.protobufVersion,

  // ScalaPB runtime (+ gRPC)
  "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
)
