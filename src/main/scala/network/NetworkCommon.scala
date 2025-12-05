package network

trait NetworkService {
  def bind(handler: (Int, Array[Byte]) => Unit): Unit
  def send(targetId: Int, data: Array[Byte]): Unit
  def connect(masterHost: String, masterPort: Int): Unit
  def stop(): Unit
  def hasPendingMessages(): Boolean
  // [추가] 현재 전송 중인(ACK 대기 중인) 메시지 개수 확인
  def getPendingCount(): Int
}
