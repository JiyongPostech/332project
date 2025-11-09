package master

import common._
import network._

// 제어 메시지 전송 어댑터 (네트워크 계층 붙일 자리)
final class NetControlSender(ms: MasterService) extends ControlSender {
  def sendAssign(workerId: Int, msg: ControlMessage.AssignTask): Unit = {
    // TODO: ms.broadcast(...) 또는 특정 워커 대상 전송
  }
  def sendCancel(workerId: Int, msg: ControlMessage.CancelTask): Unit = {
    // TODO
  }
}

object Launcher {
  def main(args: Array[String]): Unit = {
    // 1) 마스터 통신 서버 준비
    val masterNet: MasterService = /* TODO: 실제 구현 주입 */ null

    // 2) 태스크 분배 정책 + 제어 송신 어댑터 생성
    val assigner  = new RoundRobinAssigner()
    val ctl       = new NetControlSender(masterNet)

    // 3) 코디네이터 구성 및 시작
    val coord = new MasterCoordinator(masterNet, ctl)
    coord.start()

    // 4) (옵션) 워커 등록 이벤트를 코디네이터에 연결
    // masterNet.onWorkerRegistered(coord.onWorkerRegistered)  // ← 이런 식으로 콜백 연결 예정
    // masterNet.onWorkerLeft(coord.onWorkerLeft)

    // 5) 데모용 잡 하나 제출 (실제에선 외부에서 들어옴)
    val job = Job(
      id = "job-001",
      tasks = Seq(
        Task("t1", "sample/partition for part-0"),
        Task("t2", "sample/partition for part-1")
      )
    )
    coord.submitJob(job)

    // 6) 종료 대기(데모용). 실제 서비스에서는 블로킹 서버 루프
    // scala.io.StdIn.readLine()
    // coord.stop()
  }
}
