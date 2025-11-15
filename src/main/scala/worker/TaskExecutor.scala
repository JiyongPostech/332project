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

/** 
 * 분산 정렬 파이프라인을 TaskExecutor 인터페이스로 래핑
 * WorkerRuntime의 기존 구현을 재사용
 */
class SortingTaskExecutor(runtime: WorkerRuntime) extends TaskExecutor {
  override def run(task: Task): TaskResult = {
    try {
      // meta에서 operation 추출
      val operation = task.meta.getOrElse("operation", "UNKNOWN")
      
      operation match {
        case "ANALYZE" =>
          // 분석 단계: 받은 데이터 분석 후 마스터에 전송
          val result = runtime.runAnalysisFromReceivedData()
          TaskResult.Success(Seq(s"Analysis completed: ${result.count} items"))
          
        case "SHUFFLE" =>
          // Shuffle 단계: 키 범위에 따라 데이터 재분배
          val shuffledData = runtime.getReceivedData
          // runtime.shuffle()는 WorkerRuntime 내부에서 호출됨
          TaskResult.Success(Seq(s"Shuffle completed: ${shuffledData.size} items"))
          
        case "SORT" =>
          // 정렬 단계: 로컬 데이터 정렬 후 마스터에 전송
          val sortedData = runtime.getSortedData
          TaskResult.Success(Seq(s"Sort completed: ${sortedData.size} items"))
          
        case _ =>
          TaskResult.Failure(s"Unknown operation: $operation")
      }
    } catch {
      case e: Exception =>
        TaskResult.Failure(s"Task execution failed: ${e.getMessage}")
    }
  }
}
