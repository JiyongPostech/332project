package master

import common._
import network._

// 제어 메시지 전송 어댑터 (네트워크 계층 붙일 자리)
final class NetControlSender(ms: MasterService) extends ControlSender {
  def sendAssign(workerId: Int, msg: ControlMessage.AssignTask): Unit = {
    ms match {
      case netty: NettyMasterService =>
        val message = s"ASSIGN:${msg.jobId}:${msg.task.id}:${msg.task.desc}\n"
        netty.sendToWorker(workerId, message)
      case _ =>
        println(s"[NetControlSender] Cannot send to workerId=$workerId: MasterService type not supported")
    }
  }
  def sendCancel(workerId: Int, msg: ControlMessage.CancelTask): Unit = {
    ms match {
      case netty: NettyMasterService =>
        val message = s"CANCEL:${msg.jobId}:${msg.taskId}\n"
        netty.sendToWorker(workerId, message)
      case _ =>
        println(s"[NetControlSender] Cannot cancel for workerId=$workerId: MasterService type not supported")
    }
  }
}

object Launcher {
  def main(args: Array[String]): Unit = {
    // 1) 마스터 통신 서버 준비
    val masterNet: MasterService = new NettyMasterService(50051)

    // 2) 태스크 분배 정책 + 제어 송신 어댑터 생성
    val ctl       = new NetControlSender(masterNet)

    // 3) 코디네이터 구성 및 시작
    val coord = new MasterCoordinator(masterNet, ctl)
    coord.start()

    // 4) 워커 등록 이벤트를 코디네이터에 연결
    masterNet match {
      case netty: NettyMasterService =>
        netty.onWorkerRegistered(coord.onWorkerRegistered)
        netty.onWorkerLeft(coord.onWorkerLeft)
      case _ =>
    }

    // 5) 초기 데이터 분배 (워커들이 등록된 후)
    // 실제 환경에서는 워커 등록 완료 이벤트 후 호출
    println("[Master] Waiting for workers to register...")
    Thread.sleep(3000)  // 워커 등록 대기
    
    val initialDataFile = "data/master_input.txt"  // 마스터가 읽을 초기 데이터
    coord.distributeInitialData(initialDataFile)

    // 6) 데모용 잡 하나 제출 (실제에선 외부에서 들어옴)
    val job = Job(
      id = "job-001",
      tasks = Seq(
        Task("t1", "sample/partition for part-0"),
        Task("t2", "sample/partition for part-1")
      )
    )
    coord.submitJob(job)

    // 7) 종료 대기(데모용). 실제 서비스에서는 블로킹 서버 루프
    // scala.io.StdIn.readLine()
    // coord.stop()
  }
}
