package common

/** 잡과 태스크의 최소 메타데이터 정의 */
final case class Job(
  id: String,
  tasks: Seq[Task]
)

final case class Task(
  id: String,
  desc: String,
  // 파이프라인 단계/파티션ID/입력경로 등은 이후 확장
  meta: Map[String, String] = Map.empty
)

/** 워커 정보 (마스터가 보유) */
final case class WorkerInfo(
  id: Int,
  host: String,
  ports: Map[String, Int] = Map.empty   // tcp/udp/data 등 이름→포트
)

/** 태스크 실행 결과 */
sealed trait TaskResult
object TaskResult {
  final case class Success(outputs: Seq[String]) extends TaskResult
  final case class Failure(reason: String)       extends TaskResult
}
