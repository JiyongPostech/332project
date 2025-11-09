package common

/** Master ↔ Worker 제어 메시지 정의(스켈레톤) */
sealed trait ControlMessage
object ControlMessage {

  // 등록/상태
  final case class RegisterWorker(id: Int, host: String) extends ControlMessage
  final case class Heartbeat(id: Int)                    extends ControlMessage

  // 태스크 할당/취소
  final case class AssignTask(jobId: String, task: Task) extends ControlMessage
  final case class CancelTask(jobId: String, taskId: String) extends ControlMessage

  // 진행/완료/실패 보고
  final case class TaskProgress(jobId: String, taskId: String, pct: Int) extends ControlMessage
  final case class TaskFinished(jobId: String, taskId: String, outputs: Seq[String]) extends ControlMessage
  final case class TaskFailed(jobId: String, taskId: String, reason: String) extends ControlMessage
}
