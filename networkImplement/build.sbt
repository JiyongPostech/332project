// 1. 기본 설정
scalaVersion := "2.13.12"

val nettyVersion = "4.1.100.Final"

// 2. 공통 라이브러리
val commonDependencies: Seq[Setting[_]] = Seq(
  libraryDependencies ++= Seq(
    "io.netty" % "netty-all" % nettyVersion
  )
)

// 3. 명령어 별명
val commandAliases: Seq[Setting[_]] = (1 to 10).flatMap { i =>
  addCommandAlias(s"runClient$i", s"runMain RunClient$i")
} ++ addCommandAlias("runMaster", "runMain RunMaster")

// 4. 루트 프로젝트 정의
lazy val root = (project in file("."))
  .settings(
    // --- 개별 설정 ---
    name := "chatapp",
    fork := true, // 프로그램을 별도 프로세스로 실행

    // ✅✅✅ 이 설정을 추가하세요! ✅✅✅
    // 'run' 태스크를 실행할 때, 키보드 입력(stdin)을 연결합니다.
    (run / connectInput) := true
  )
  // --- 설정 시퀀스(Seq) 추가 ---
  .settings(commonDependencies)
  .settings(commandAliases)