package network

trait NetworkService {
  /**
   * 네트워크 시작 및 콜백 등록
   * @param handler (senderId, payload) => Unit
   */
  def bind(handler: (Int, Array[Byte]) => Unit): Unit

  /**
   * 데이터 전송 (목적지에 따라 TCP/UDP 자동 라우팅)
   * @param targetId 목적지 ID (0: Master, 그 외: Worker)
   * @param data 보낼 바이트 배열
   */
  def send(targetId: Int, data: Array[Byte]): Unit

  /**
   * 마스터 연결 (워커용)
   */
  def connect(masterHost: String, masterPort: Int): Unit

  /**
   * 네트워크 종료
   */
  def stop(): Unit
}