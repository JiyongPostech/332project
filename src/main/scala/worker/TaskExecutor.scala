package worker

import common._

/** 태스크 실제 실행기 — 파이프라인 구현은 다음 마일스톤에서 주입 */
trait TaskExecutor {
  def run(task: Task): TaskResult
}

/** 더미 실행기(스켈레톤 테스트용) */
class NoopTaskExecutor extends TaskExecutor {
  override def run(task: Task): TaskResult = TaskResult.Success(Nil)
}
