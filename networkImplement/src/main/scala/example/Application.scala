// src/main/scala/Application.scala

import java.net.InetSocketAddress
import scala.collection.concurrent.TrieMap
import scala.io.StdIn

// ######################################################################
// ##                                                                  ##
// ##                   채팅 애플리케이션 로직 (ChatClient)                  ##
// ##                                                                  ##
// ######################################################################

class ChatClient(myId: Int, networkService: NetworkService) { // Netty를 모름!

  private val activePeers: TrieMap[Int, InetSocketAddress] = TrieMap[Int, InetSocketAddress]()

  // --- 네트워크 콜백 정의 ---
  private def onP2PMessage(senderId: Int, message: String): Unit = {
    println(s"\n[${Console.BLUE}ID:$myId 채널${Console.RESET}] $senderId: $message")
    printChatPrompt()
  }
  private def onPeerListReceived(peers: Map[Int, InetSocketAddress]): Unit = {
    println(s"\n[${Console.GREEN}ID:$myId 시스템${Console.RESET}] 초기 피어 목록 (${peers.size}개): ${peers.keys.mkString(", ")}")
    activePeers.clear(); activePeers ++= peers
    activePeers.keys.filter(_ != myId).foreach { peerId =>
      networkService.send(peerId, s"[$myId] 님이 채팅에 참여했습니다.")
    }
    printChatPrompt()
  }
  private def onPeerJoined(id: Int, addr: InetSocketAddress): Unit = {
    println(s"\n[${Console.GREEN}ID:$myId 시스템${Console.RESET}] 새로운 피어 접속: $id ($addr)")
    activePeers.put(id, addr)
    printChatPrompt()
  }
  private def onPeerLeft(id: Int): Unit = {
    println(s"\n[${Console.GREEN}ID:$myId 시스템${Console.RESET}] 피어 접속 종료: $id")
    activePeers.remove(id)
    printChatPrompt()
  }
  private def printChatPrompt(): Unit = {
    print(s"[${Console.YELLOW}ID:$myId 당신${Console.RESET}] 현재 접속: ${activePeers.keys.filter(_ != myId).mkString(", ")} > ")
  }

  def start(): Unit = {
    println(s"===== ${Console.MAGENTA}ID:$myId 채팅 클라이언트 시작${Console.RESET} =====")
    
    // 1. P2P 메시지 수신 준비
    networkService.bind(onP2PMessage)

    // 2. 마스터 서버에 접속
    networkService.connect_to_master(
      onPeerListReceived, onPeerJoined, onPeerLeft
    )

    // 3. 사용자 입력 처리
// 3. 사용자 입력 처리
    try {
      var input: String = null
      printChatPrompt() // 루프 시작 전 프롬프트 먼저 출력

      // ✅ FIX: readLine이 null(EOF)을 반환하는 경우를 처리하도록 while 조건 변경
      while ({input = StdIn.readLine(); input != null && input != "exit"}) {
        
        // '/to ID 메시지' 형식
        val parts = input.split(" ", 3)
        if (parts.length == 3 && parts(0) == "/to") {
           parts(1).toIntOption.foreach { id =>
             if (activePeers.contains(id)) {
               networkService.send(id, s"[귓속말] ${parts(2)}")
             } else {
               println(s"[${Console.RED}시스템${Console.RESET}] $id 님은 접속 중이 아닙니다.")
             }
           }
        } else if (input.trim.nonEmpty) { // ✅ FIX: 빈 메시지는 보내지 않음
          // 전체 메시지
          activePeers.keys.filter(_ != myId).foreach { peerId =>
            networkService.send(peerId, s"$input")
          }
        }

        // 다음 입력을 받기 전에 프롬프트 다시 출력
        printChatPrompt()
      }
    } finally {
      // 루프가 종료되면 (exit를 입력했거나, null을 받았을 때)
      networkService.stop()
      println(s"\n[${Console.GREEN}ID:$myId 시스템${Console.RESET}] 채팅 클라이언트 종료.")
    }
  }
}

// ######################################################################
// ##                                                                  ##
// ##                    실행 객체 (Master / Clients)                    ##
// ##                                                                  ##
// ######################################################################

object RunMaster {
  def main(args: Array[String]): Unit = {
    // ✅ "설계도" 타입으로 선언하고, "Netty 구현체"를 주입합니다.
    val server: MasterService = new NettyMasterService(8000)
    
    // 나중에 gRPC로 바꾸려면?
    // val server: MasterService = new GrpcMasterService(8000)
    
    server.start()
  }
}

/**
 * 10개의 클라이언트를 실행하기 위한 템플릿.
 * (build.sbt에서 runClient1 ... runClient10으로 실행)
 */
trait ClientRunner {
  val MASTER_HOST = "127.0.0.1"
  val MASTER_PORT = 8000
  val MY_ID: Int // 각 객체에서 정의
  lazy val MY_UDP_PORT: Int = 9000 + MY_ID

  def main(args: Array[String]): Unit = {
    
    // ✅ "설계도" 타입으로 선언하고, "Netty 구현체"를 주입합니다.
    val network: NetworkService = new NettyClientService(
      myId = MY_ID,
      myUdpPort = MY_UDP_PORT,
      masterHost = MASTER_HOST,
      masterPort = MASTER_PORT
    )
    
    // 나중에 gRPC로 바꾸려면?
    // val network: NetworkService = new GrpcClientService(...)

    // ChatClient는 자기가 Netty를 쓰는지 gRPC를 쓰는지 전혀 모릅니다.
    val chatClient = new ChatClient(MY_ID, network)
    chatClient.start()
  }
}

// 10개의 실행 객체
object RunClient1 extends ClientRunner { override val MY_ID = 1 }
object RunClient2 extends ClientRunner { override val MY_ID = 2 }
object RunClient3 extends ClientRunner { override val MY_ID = 3 }
object RunClient4 extends ClientRunner { override val MY_ID = 4 }
object RunClient5 extends ClientRunner { override val MY_ID = 5 }
object RunClient6 extends ClientRunner { override val MY_ID = 6 }
object RunClient7 extends ClientRunner { override val MY_ID = 7 }
object RunClient8 extends ClientRunner { override val MY_ID = 8 }
object RunClient9 extends ClientRunner { override val MY_ID = 9 }
object RunClient10 extends ClientRunner { override val MY_ID = 10 }
