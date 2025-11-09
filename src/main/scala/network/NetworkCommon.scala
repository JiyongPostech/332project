// src/main/scala/NetworkCommon.scala

import java.net.InetSocketAddress

// ---------------------------------------------
// 1. 공통 언어 (프로토콜)
// ---------------------------------------------

// Client -> Master
case class Register(id: Int, udpPort: Int)

// Master -> Client
sealed trait MasterMessage
case class PeerList(peers: Map[Int, InetSocketAddress]) extends MasterMessage
case class PeerJoined(id: Int, address: InetSocketAddress) extends MasterMessage
case class PeerLeft(id: Int) extends MasterMessage

/**
 * 마스터와 클라이언트 간의 메시지를 인코딩/디코딩하는 객체
 * 모든 구현체(Netty, gRPC)는 이 객체를 사용하여 통신 언어를 통일합니다.
 */
object Codec {
  // C -> M
  def encodeRegister(reg: Register): String = s"REGISTER:${reg.id}:${reg.udpPort}\n"
  
  // C -> M (마스터가 메시지 파싱)
  def decodeFromClient(line: String): Option[Register] = {
    line.split(":") match {
      case Array("REGISTER", id, port) => Some(Register(id.toInt, port.toInt))
      case _ => None
    }
  }
  
  // M -> C
  def encodeMessage(msg: MasterMessage): String = {
    msg match {
      case PeerList(peers) =>
        val peerStr = peers.map { case (id, addr) => s"$id@${addr.getAddress.getHostAddress}:${addr.getPort}" }.mkString(",")
        s"PEER_LIST:$peerStr\n"
      case PeerJoined(id, addr) =>
        s"PEER_JOINED:$id@${addr.getAddress.getHostAddress}:${addr.getPort}\n"
      case PeerLeft(id) =>
        s"PEER_LEFT:$id\n"
    }
  }

  // M -> C (클라이언트가 메시지 파싱)
  def decodeFromMaster(line: String): Option[MasterMessage] = {
    val parts = line.split(":", 2) // "CMD:Payload"
    parts.head match {
      case "PEER_LIST" =>
        val peers = parts(1).split(",").filter(_.nonEmpty).map { peerStr =>
          val Array(id, fullAddr) = peerStr.split("@")
          val Array(host, port) = fullAddr.split(":")
          (id.toInt -> new InetSocketAddress(host, port.toInt))
        }.toMap
        Some(PeerList(peers))
      
      case "PEER_JOINED" =>
        val Array(id, fullAddr) = parts(1).split("@")
        val Array(host, port) = fullAddr.split(":")
        Some(PeerJoined(id.toInt, new InetSocketAddress(host, port.toInt)))
        
      case "PEER_LEFT" =>
        Some(PeerLeft(parts(1).toInt))
        
      case _ => None
    }
  }
}


// ---------------------------------------------
// 2. 서버/클라이언트 설계도 (Traits)
// ---------------------------------------------

/**
 * 마스터 서버 구현체를 위한 설계도
 */
trait MasterService {
  def start(): Unit
  def stop(): Unit
}

/**
 * 클라이언트 구현체를 위한 설계도
 */
trait NetworkService {
  def bind(onMessageReceived: (Int, String) => Unit): Unit
  def connect_to_master(
    onPeerListReceived: (Map[Int, InetSocketAddress]) => Unit,
    onPeerJoined: (Int, InetSocketAddress) => Unit,
    onPeerLeft: (Int) => Unit
  ): Unit
  def send(targetId: Int, message: String): Unit
  def stop(): Unit
}
