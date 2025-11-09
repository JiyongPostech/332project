package master

import common._
import network._   // 기존 MasterService / NetworkService 활용 (start/stop 등)

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** 전체 Job 흐름을 ‘조정’하는 진입점 */
class MasterCoordinator(masterNet: MasterService, sendCtl: ControlSender) {

  private val workers   = new ConcurrentHashMap[Int, WorkerInfo]().asScala // id -> info
  private val jobStates = new ConcurrentHashMap[String, JobState]().asScala

  /** 마스터 프로세스 시작 (네트워킹 서버 가동 등) */
  def start(): Unit = {
    masterNet.start() // 기존 트레이트 사용 
  }

  /** 안전 종료 */
  def stop(): Unit = masterNet.stop()

  /** 워커 등록/해제 이벤트 처리 (네트워크 계층에서 콜백 연결 예정) */
  def onWorkerRegistered(info: WorkerInfo): Unit = {
    workers.put(info.id, info)
  }
  def onWorkerLeft(workerId: Int): Unit = {
    workers.remove(workerId)
  }

  /** 새로운 Job 수신 → 상태 생성 → 태스크 분해 → 배정 */
  def submitJob(job: Job): Unit = {
    val state = JobState(jobId = job.id, totalTasks = job.tasks.size)
    jobStates.put(job.id, state)
    TaskAssigner.assignAll(job, workers.values.toSeq, sendCtl, onTaskProgress, onTaskFinished, onTaskFailed)
  }

  /** 태스크 진행/완료/실패 콜백 */
  private def onTaskProgress(jobId: String, taskId: String, pct: Int): Unit = {
    jobStates.get(jobId).foreach(_.progress.update(taskId, pct))
  }
  private def onTaskFinished(jobId: String, taskId: String, outputs: Seq[String]): Unit = {
    jobStates.get(jobId).foreach { s =>
      s.completedTasks += 1
      s.outputs ++= outputs
      if (s.completedTasks == s.totalTasks) onJobCompleted(jobId, s.outputs.toSeq)
    }
  }
  private def onTaskFailed(jobId: String, taskId: String, reason: String): Unit = {
    // 재시도 정책은 이후 단계에서 구현
  }

  private def onJobCompleted(jobId: String, outputs: Seq[String]): Unit = {
    // 최종 결과 집계/보고 (문서/로그/상위 호출 등)
  }
}

/** 잡 실행 중 마스터가 들고 있는 상태 */
final case class JobState(
  jobId: String,
  totalTasks: Int,
  var completedTasks: Int = 0,
  outputs: collection.mutable.ArrayBuffer[String] = collection.mutable.ArrayBuffer.empty,
  progress: collection.mutable.Map[String, Int] = collection.mutable.HashMap.empty
)

/** 제어 메시지 송신을 추상화 (네트워크 계층에 의존 줄이기) */
trait ControlSender {
  def sendAssign(workerId: Int, msg: ControlMessage.AssignTask): Unit
  def sendCancel(workerId: Int, msg: ControlMessage.CancelTask): Unit
}
