package common

object Messages {
  val TYPE_DATA = "DATA"
  val TYPE_ACK  = "ACK"
  val TYPE_FIN  = "FIN"
  
  val TYPE_REGISTER    = "REGISTER"
  val TYPE_PEER_LIST   = "PEER_LIST"
  val TYPE_PEER_JOINED = "PEER_JOINED"
  
  val TYPE_SAMPLE   = "SAMPLE"
  val TYPE_RANGE    = "RANGE"
  val TYPE_DONE     = "DONE"      // 워커 -> 마스터 (나 끝났어)
  val TYPE_ALL_DONE = "ALL_DONE"  // 마스터 -> 워커 (전원 끝났으니 퇴근해)
  
  val DELIMITER = ":"
}