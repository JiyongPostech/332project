package worker

import common._
import network._  // 기존 NetworkService 활용 (bind, connect_to_master, send, stop) 

/** 워커 프로세스의 생명주기/이벤트 루프 */
class WorkerRuntime(
  id: Int,
  net: NetworkService,
  exec: TaskExecutor
) {

  /** 시작: 마스터에 등록, 제어 메시지 수신 바인딩 */
  def start(): Unit = {
    // P2P 메시지 수신(데이터 평면)은 이후 필요 시 bind 사용
    net.bind(onPeerMessage)
    // 마스터 접속 및 컨트롤 메시지 콜백 연결은 네트워크 계층 확장 시 주입 예정
    net.connect_to_master(onInitialPeers, onPeerJoined, onPeerLeft)
    // 별도: 등록 메시지 전송(프로토콜은 ControlMessage.RegisterWorker 등으로 통일)
  }

  /** 안전 종료 */
  def stop(): Unit = net.stop()

  /** (옵션) P2P 텍스트 수신 콜백 — 현 단계에선 제어 메시지로 대체 예정 */
  private def onPeerMessage(senderId: Int, msg: String): Unit = {
    // 필요 시 디버깅/로그
  }
  private def onInitialPeers(peers: Map[Int, java.net.InetSocketAddress]): Unit = {}
  private def onPeerJoined(id: Int, addr: java.net.InetSocketAddress): Unit = {}
  private def onPeerLeft(id: Int): Unit = {}

  /** 마스터로부터 태스크 할당 수신 시 호출(네트워크 계층에서 라우팅) */
  def onAssign(msg: ControlMessage.AssignTask): Unit = {
    report(ControlMessage.TaskProgress(msg.jobId, msg.task.id, 0))
    val result = exec.run(msg.task) // 실제 파이프라인 로직은 다음 마일스톤에서 구현
    result match {
      case TaskResult.Success(out) =>
        report(ControlMessage.TaskFinished(msg.jobId, msg.task.id, out))
      case TaskResult.Failure(reason) =>
        report(ControlMessage.TaskFailed(msg.jobId, msg.task.id, reason))
    }
  }

  /** 진행/완료/실패 보고 (네트워크 계층으로 전달) */
  private def report(msg: ControlMessage): Unit = {
    // net.send(...) 또는 별도의 제어 채널로 전송하도록 확장
  }
}
