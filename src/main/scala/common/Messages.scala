package common

/** Master ↔ Worker 제어 메시지 정의 (레거시, 현재 미사용) */
sealed trait ControlMessage
object ControlMessage {
  // 태스크 할당 (현재는 전송만 하고 처리 안 함, 추후 확장용)
  final case class AssignTask(jobId: String, task: Task) extends ControlMessage
  final case class CancelTask(jobId: String, taskId: String) extends ControlMessage
}
