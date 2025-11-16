// src/main/scala/network/NettyImplementation.scala
package network

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel._
import io.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.{DatagramPacket, SocketChannel}
import io.netty.channel.socket.nio.{NioDatagramChannel, NioServerSocketChannel, NioSocketChannel}
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.util.CharsetUtil
import io.netty.buffer.Unpooled
import io.netty.util.concurrent.GlobalEventExecutor
import java.net.InetSocketAddress
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._ // <--- [수정 1] .asScala를 위한 임포트 추가

// ######################################################################
// ##                                                                  ##
// ##               1. Netty 마스터 서버 구현 (수정)
// ##                                                                  ##
// ######################################################################

/** [Netty-Private] 마스터 서버의 상태 (수정) */
private class NettyMasterState {
  val allClients: ChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  val peerMap: TrieMap[Int, InetSocketAddress] = TrieMap[Int, InetSocketAddress]() // 현재 '활성' 피어
  val channelToIdMap: TrieMap[Channel, Int] = TrieMap[Channel, Int]()
  
  // (수정) 마스터가 '기대하는' 워커 ID 목록 (재시작해도 기다려주기 위함)
  // [수정 1] .asScala를 사용하기 위해 임포트 추가 (파일 상단)
  val expectedWorkerIds: mutable.Set[Int] = ConcurrentHashMap.newKeySet[Int]().asScala
  
  // 수집된 분석 결과: workerId -> (min, max, count)
  val analysisResults: TrieMap[Int, (Long, Long, Int)] = TrieMap[Int, (Long, Long, Int)]()
  // 분석 결과와 전역 샘플 버퍼
  val samples: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty[Long]
  // 각 워커로부터 받은 정렬된 데이터 청크
  val sortedChunks: TrieMap[Int, mutable.ArrayBuffer[Long]] = TrieMap[Int, mutable.ArrayBuffer[Long]]()
  // 청크 수신 추적: workerId -> (totalCount, receivedCount)
  val chunkTracking: TrieMap[Int, (Int, Int)] = TrieMap[Int, (Int, Int)]()
  
  // 비즈니스 로직 콜백 (MasterCoordinator 연결용)
  var onWorkerRegisteredCallback: Option[common.WorkerInfo => Unit] = None
  var onWorkerLeftCallback: Option[Int => Unit] = None
}

/** [Netty-Private] 마스터 서버의 Netty 핸들러 (수정) */
private class NettyMasterHandler(state: NettyMasterState) extends SimpleChannelInboundHandler[String] {
  
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    state.allClients.add(ctx.channel)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    state.allClients.remove(ctx.channel)
    state.channelToIdMap.remove(ctx.channel).foreach { id =>
      state.peerMap.remove(id) // (수정) '활성' 피어 목록에서만 제거
      state.allClients.writeAndFlush(Codec.encodeMessage(PeerLeft(id)))
      // (수정) expectedWorkerIds 에서는 제거하지 않음! (부활을 기다림)
      state.onWorkerLeftCallback.foreach(_(id))
      println(s"마스터: 워커 $id 연결 끊김 (peerMap에서 제거). 부활 대기.")
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    Codec.decodeFromClient(msg) match {
      case Some(Register(id, udpPort)) =>
        val remoteIp = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
        val udpAddress = new InetSocketAddress(remoteIp, udpPort)
        
        ctx.writeAndFlush(Codec.encodeMessage(PeerList(state.peerMap.toMap)))
        
        state.peerMap.put(id, udpAddress)
        state.channelToIdMap.put(ctx.channel, id)
        
        // (수정) 마스터가 기대하는 워커 목록에 추가 (재시작 시에도 동일)
        state.expectedWorkerIds.add(id)
        println(s"마스터: 워커 $id 등록 (expectedWorkerIds에 추가/확인)")
        
        val joinMsg = Codec.encodeMessage(PeerJoined(id, udpAddress))
        state.allClients.writeAndFlush(joinMsg, (ch: Channel) => ch != ctx.channel)
        
        // 비즈니스 로직 콜백 호출
        state.onWorkerRegisteredCallback.foreach { callback =>
          import common.WorkerInfo
          callback(WorkerInfo(id, remoteIp, Map("udp" -> udpPort)))
        }
        
      case None =>
        // 문자열 기반 커스텀 메시지 처리(분석 결과)
        if (msg.startsWith("ANALYSIS:")) {
          val body = msg.stripPrefix("ANALYSIS:").trim
          val parts = body.split(":", 4)
          if (parts.length >= 3) {
            state.channelToIdMap.get(ctx.channel).foreach { wid =>
              state.synchronized {
                // 첫 번째 워커의 분석 결과 수신 시 샘플 버퍼 초기화
                if (state.analysisResults.isEmpty) {
                  state.samples.clear()
                }
                
                val minV = parts(0).toLong
                val maxV = parts(1).toLong
                val cnt  = parts(2).toInt
                state.analysisResults.put(wid, (minV, maxV, cnt))

                // 샘플 파싱 및 축적 (옵션 필드)
                if (parts.length == 4 && parts(3).nonEmpty) {
                  parts(3).split(",").filter(_.nonEmpty).foreach { s =>
                    try state.samples += s.toLong catch { case _: Throwable => () }
                  }
                }

                // (수정) 완료 조건: 'peerMap.keySet' 대신 'expectedWorkerIds'와 비교
                if (state.expectedWorkerIds.nonEmpty && state.analysisResults.keySet.equals(state.expectedWorkerIds)) {
                  println(s"마스터: 모든 ${state.expectedWorkerIds.size}개 '기대' 워커로부터 분석 결과 수신 완료.")
                  val globalMin = state.analysisResults.values.map(_._1).min
                  val globalMax = state.analysisResults.values.map(_._2).max
                  val workers = state.expectedWorkerIds.toSeq.sorted // 'expectedWorkerIds' 기준
                  val ranges = computeQuantileRanges(globalMin, globalMax, workers, state.samples.toSeq)
                  val msgStr = Codec.encodeMessage(KeyRangeUpdate(ranges))
                  state.allClients.writeAndFlush(msgStr)
                  state.samples.clear()
                }
              }
            }
          }
        } else if (msg.startsWith("SORTED:")) {
          // 워커로부터 정렬된 데이터 수신 (최종 병합을 위해)
          val body = msg.stripPrefix("SORTED:").trim
          val parts = body.split(":", 4)
          if (parts.length >= 4) {
            val workerId = parts(0).toInt
            val totalCount = parts(1).toInt
            val chunkIdx = parts(2).toInt
            val dataStr = parts(3)
            val data = if (dataStr.nonEmpty) dataStr.split(",").map(_.toLong).toSeq else Seq.empty
            
            // 데이터 축적
            state.sortedChunks.getOrElseUpdate(workerId, mutable.ArrayBuffer.empty) ++= data
            
            // 청크 추적 업데이트
            val (prevTotal, prevCount) = state.chunkTracking.getOrElse(workerId, (totalCount, 0))
            val newCount = prevCount + 1
            state.chunkTracking.put(workerId, (totalCount, newCount))
            
            println(s"마스터: 워커 $workerId 로부터 청크 $chunkIdx 수신 (${data.size}개, 총 ${state.sortedChunks(workerId).size}/$totalCount)")
            
            // (수정) 완료 조건: 'peerMap.keySet' 대신 'expectedWorkerIds'와 비교
            val allWorkersPresent = state.expectedWorkerIds.nonEmpty && state.sortedChunks.keySet.equals(state.expectedWorkerIds)
            
            val allChunksReceived = state.chunkTracking.nonEmpty && state.chunkTracking.forall { case (wid, (totalCount, receivedCount)) =>
              // 실제 받은 데이터 크기가 선언된 총 개수와 일치하는지 확인
              state.sortedChunks.get(wid).exists { chunks =>
                // (수정) 청크 수신 완료 조건 단순화 (총 개수 일치)
                chunks.size == totalCount
              }
            }
            
            if (allWorkersPresent && allChunksReceived) {
              println(s"마스터: 모든 ${state.expectedWorkerIds.size}개 '기대' 워커로부터 정렬 데이터 수신 완료. 최종 병합 시작.")
              // K-way 병합 수행
              val mergedData = mergeKSortedArrays(state.sortedChunks.values.map(_.toSeq).toSeq)
              println(s"마스터: 최종 병합 완료 (총 ${mergedData.size}개)")
              
              // 최종 결과를 파일로 저장
              val outputPath = "sorted_result.txt"
              try {
                val writer = new java.io.PrintWriter(outputPath)
                try {
                  mergedData.foreach(writer.println)
                  println(s"마스터: 정렬 결과를 $outputPath 에 저장했습니다.")
                } finally {
                  writer.close()
                }
              } catch {
                case e: Exception => 
                  println(s"결과 저장 실패: ${e.getMessage}")
              }
              
              // 정리
              state.sortedChunks.clear()
              state.chunkTracking.clear()
            }
          }
        } else {
          println(s"알 수 없는 메시지: $msg")
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  private def computeQuantileRanges(globalMin: Long, globalMax: Long, workers: Seq[Int], samples: Seq[Long]): Seq[(Int, Long, Long)] = {
    if (workers.isEmpty) return Seq.empty
    if (samples.nonEmpty) {
      val k = workers.size
      val sorted = samples.sorted
      val pivots = (1 until k).map { j =>
        val idx = math.min(sorted.size - 1, math.max(0, math.ceil(j * sorted.size.toDouble / k).toInt - 1))
        sorted(idx)
      }.distinct
      val lows  = globalMin +: pivots
      val highs = pivots :+ globalMax
      workers.zip(lows.zip(highs)).map { case (wid, (lo, hi)) => (wid, lo, hi) }
    } else {
      val k = workers.size
      val width = math.max(1L, (globalMax - globalMin + 1) / k)
      workers.zipWithIndex.map { case (wid, idx) =>
        val start = globalMin + idx * width
        val end   = if (idx == k - 1) globalMax else (start + width - 1)
        (wid, start, end)
      }
    }
  }
  
  /**
   * K개의 정렬된 배열을 병합 (우선순위 큐 사용)
   */
  private def mergeKSortedArrays(sortedArrays: Seq[Seq[Long]]): Seq[Long] = {
    import scala.collection.mutable.PriorityQueue
    case class Item(value: Long, arrayIdx: Int, elemIdx: Int)
    implicit val ord: Ordering[Item] = Ordering.by[Item, Long](_.value).reverse
    
    val pq = PriorityQueue[Item]()
    val result = mutable.ArrayBuffer[Long]()
    
    // 각 배열의 첫 요소를 우선순위 큐에 삽입
    sortedArrays.zipWithIndex.foreach { case (arr, idx) =>
      if (arr.nonEmpty) pq.enqueue(Item(arr.head, idx, 0))
    }
    
    // 우선순위 큐에서 최솟값을 꺼내고 다음 요소 삽입
    while (pq.nonEmpty) {
      val Item(value, arrayIdx, elemIdx) = pq.dequeue()
      result += value
      
      val nextIdx = elemIdx + 1
      if (nextIdx < sortedArrays(arrayIdx).size) {
        pq.enqueue(Item(sortedArrays(arrayIdx)(nextIdx), arrayIdx, nextIdx))
      }
    }
    
    result.toSeq
  }
}

/**
 * (Public) Netty 마스터 서버 구현체
 * NetworkCommon.scala의 MasterService 설계도를 구현합니다.
 */
class NettyMasterService(port: Int) extends MasterService {
  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()
  private val state = new NettyMasterState()
  private var channel: Channel = _

  override def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline()
              .addLast(new LineBasedFrameDecoder(8192))
              .addLast(new StringDecoder(CharsetUtil.UTF_8))
              .addLast(new StringEncoder(CharsetUtil.UTF_8))
              .addLast(new NettyMasterHandler(state))
          }
        })
      channel = b.bind(port).sync().channel()
      println(s"Netty 마스터 서버가 포트 $port 에서 시작되었습니다.")
    } catch {
      case e: Exception => 
        e.printStackTrace()
        stop()
    }
  }
  
  override def stop(): Unit = {
    if (channel != null) channel.close()
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    println("Netty 마스터 서버가 종료되었습니다.")
  }
  
  /** 특정 워커에게 메시지 전송 */
  override def sendToWorker(workerId: Int, message: String): Unit = {
    state.channelToIdMap.collectFirst {
      case (ch, id) if id == workerId && ch.isActive => ch
    }.foreach { ch =>
      val msg = if (message.endsWith("\n")) message else message + "\n"
      ch.writeAndFlush(msg)
    }
  }
  
  /** 워커 등록 콜백 연결 */
  def onWorkerRegistered(callback: common.WorkerInfo => Unit): Unit = {
    state.onWorkerRegisteredCallback = Some(callback)
  }
  
  /** 워커 해제 콜백 연결 */
  def onWorkerLeft(callback: Int => Unit): Unit = {
    state.onWorkerLeftCallback = Some(callback)
  }
}


// ######################################################################
// ##                                                                  ##
// ##              2. Netty 클라이언트 서비스 구현 (핵심 수정)
// ##                                                                  ##
// ######################################################################

/** 신뢰성 있는 전송을 위한 메시지 래퍼 (내부용) */
private case class ReliableMessage(targetId: Int, payload: String, msgId: String, var retryCount: Int = 0, var lastSentTime: Long = 0)

/** [Netty-Private] 클라이언트의 상태 (신뢰성 큐 추가) */
private class NettyClientState {
  val peers: TrieMap[Int, InetSocketAddress] = TrieMap[Int, InetSocketAddress]()
  val reversePeers: TrieMap[InetSocketAddress, Int] = TrieMap[InetSocketAddress, Int]()
  
  val messageIdCounter = new AtomicLong(0)
  
  // 1. Send 큐 (WorkerRuntime -> Netty)
  // (대상ID, 순수 Payload)
  val sendQueue = new LinkedBlockingQueue[(Int, String)]()
  
  // 2. Receive 큐 (Netty I/O -> WorkerRuntime)
  // (송신자ID, 순수 Payload)
  val receiveQueue = new LinkedBlockingQueue[(Int, String)]()
  
  // 3. Un-ACKed 맵 (신뢰성 보장용)
  val unAckedMessages = new ConcurrentHashMap[String, ReliableMessage]()

  // 콜백 함수
  var onMessageCallback: (Int, String) => Unit = (_, _) => {}
  var onPeerListCallback: (Map[Int, InetSocketAddress]) => Unit = (_) => {}
  var onPeerJoinedCallback: (Int, InetSocketAddress) => Unit = (_, _) => {}
  var onPeerLeftCallback: (Int) => Unit = (_) => {}
  var onKeyRangeCallback: Seq[(Int, Long, Long)] => Unit = (_: Seq[(Int, Long, Long)]) => {}
  var onInitialDataCallback: String => Unit = (_) => {} 
  
  def handlePeerList(initialPeers: Map[Int, InetSocketAddress]): Unit = {
    peers.clear(); reversePeers.clear()
    peers ++= initialPeers
    initialPeers.foreach { case (id, addr) => reversePeers.put(addr, id) }
    onPeerListCallback(initialPeers)
  }
  def handlePeerJoined(id: Int, addr: InetSocketAddress): Unit = {
    peers.put(id, addr); reversePeers.put(addr, id)
    onPeerJoinedCallback(id, addr)
  }
  def handlePeerLeft(id: Int): Unit = {
    peers.remove(id).foreach(reversePeers.remove)
    onPeerLeftCallback(id)
  }
}

/** [Netty-Private] 클라이언트의 TCP (마스터 접속용) 핸들러 (수정) */
private class NettyClientTcpHandler(
  myId: Int,
  myUdpPort: Int,
  state: NettyClientState
) extends SimpleChannelInboundHandler[String] {
  
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.writeAndFlush(Codec.encodeRegister(Register(myId, myUdpPort)))
  }
  
  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    // (수정) EpochUpdate 제거. 이 모델에서는 Fencing이 필요 없음.
    Codec.decodeFromMaster(msg) match {
      case Some(PeerList(initialPeers)) => state.handlePeerList(initialPeers)
      case Some(PeerJoined(id, addr)) => state.handlePeerJoined(id, addr)
      case Some(PeerLeft(id)) => state.handlePeerLeft(id)
      case Some(KeyRangeUpdate(ranges)) => state.onKeyRangeCallback(ranges)
      case None =>
        // Codec에 없는 커스텀 메시지 처리 (INITIAL_DATA 등)
        if (msg.startsWith("INITIAL_DATA:")) {
          state.onInitialDataCallback(msg)
        } else {
          println(s"마스터로부터 알 수 없는 메시지: $msg")
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    println("마스터 접속 오류: " + cause.getMessage)
    ctx.close()
  }
}

/** [Netty-Private] 클라이언트의 UDP (P2P 통신용) 핸들러 (수정) */
private class NettyClientUdpHandler(state: NettyClientState) extends SimpleChannelInboundHandler[DatagramPacket] {
  
  override def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket): Unit = {
    val message = packet.content().toString(CharsetUtil.UTF_8)
    state.reversePeers.get(packet.sender()).foreach { senderId =>
      
      val parts = message.split(":", 3) // "TYPE:msgId:payload"
      if (parts.length < 2) {
        println(s"잘못된 UDP 메시지 수신: $message")
        return 
      }
      
      val msgType = parts(0)
      val msgId = parts(1)

      msgType match {
        case "ACK" =>
          // "ACK:msgId"
          state.unAckedMessages.remove(msgId)
          
        case "DATA" =>
          // "DATA:msgId:payload"
          val payload = if (parts.length == 3) parts(2) else ""
          
          // 1. ACK 전송 (I/O 스레드가 즉시)
          val ackMsg = s"ACK:$msgId"
          val ackBuffer = Unpooled.copiedBuffer(ackMsg, CharsetUtil.UTF_8)
          val ackPacket = new DatagramPacket(ackBuffer, packet.sender())
          ctx.writeAndFlush(ackPacket)

          // 2. Receive 큐에 삽입 (I/O 스레드 해방)
          // (중복 수신 방지는 큐에 넣기 전에 Set으로 체크할 수 있으나 일단 생략)
          state.receiveQueue.put((senderId, payload))
          
        case _ =>
          println(s"Unknown UDP message type: $msgType")
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
  }
}

/**
 * (Public) Netty 클라이언트 서비스 구현체
 * NetworkCommon.scala의 NetworkService 설계도를 구현합니다. (수정)
 */
class NettyClientService(
  myId: Int,
  myUdpPort: Int,
  masterHost: String,
  masterPort: Int
) extends NetworkService {
  
  private val state = new NettyClientState()
  private val tcpGroup = new NioEventLoopGroup()
  private val udpGroup = new NioEventLoopGroup()
  private var tcpChannel: Channel = _
  private var udpChannel: Channel = _
  
  // (수정) Send/Receive/Retry를 위한 스케줄러
  private val scheduler = Executors.newScheduledThreadPool(3)
  
  // 마스터로의 TCP 전송 헬퍼
  private def sendTcpToMaster(message: String): Unit = {
    if (tcpChannel != null && tcpChannel.isActive) {
      val msg = if (message.endsWith("\n")) message else message + "\n"
      tcpChannel.writeAndFlush(msg)
    } else {
      println("TCP 채널이 활성화되어 있지 않아 마스터 전송 실패")
    }
  }

  /** (수정) Receive 큐 소비자 스레드 시작 */
  override def bind(onMessageReceived: (Int, String) => Unit): Unit = {
    state.onMessageCallback = onMessageReceived
    try {
      val b = new Bootstrap()
      b.group(udpGroup).channel(classOf[NioDatagramChannel])
        .handler(new NettyClientUdpHandler(state))
      udpChannel = b.bind(myUdpPort).sync().channel()
      
      // 1. "Receive 스레드" 시작 (Receive 큐 -> WorkerRuntime)
      scheduler.execute(() => runReceiveLoop())
      
      println(s"Netty UDP: 포트 $myUdpPort 바인딩 및 Receive 스레드 시작.")
    } catch {
      case e: Exception => 
        println(s"UDP 바인딩 실패: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /** [Private] Receive 큐를 처리하는 소비자 스레드 로직 */
  private def runReceiveLoop(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        // 큐에서 (송신자ID, 순수 Payload)를 꺼냄
        val (senderId, payload) = state.receiveQueue.take()
        // I/O 스레드가 아닌 이 스레드에서 WorkerRuntime의 무거운 콜백 실행
        try {
          state.onMessageCallback(senderId, payload)
        } catch {
          case NonFatal(e) => println(s"onMessageCallback 오류: $e")
        }
      }
    } catch { case _: InterruptedException => println("Receive 스레드 종료.") }
  }

  /** (수정) Send/Retry 스레드 시작 */
  override def connect_to_master(
    onPeerListReceived: (Map[Int, InetSocketAddress]) => Unit,
    onPeerJoined: (Int, InetSocketAddress) => Unit,
    onPeerLeft: (Int) => Unit,
    onKeyRange: Seq[(Int, Long, Long)] => Unit,
    onInitialData: String => Unit
  ): Unit = {
    state.onPeerListCallback = onPeerListReceived
    state.onPeerJoinedCallback = onPeerJoined
    state.onPeerLeftCallback = onPeerLeft
    state.onKeyRangeCallback = onKeyRange
    state.onInitialDataCallback = onInitialData
    
    val tcpHandler = new NettyClientTcpHandler(myId, myUdpPort, state)

    try {
      val b = new Bootstrap()
      b.group(tcpGroup).channel(classOf[NioSocketChannel])
        .handler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline()
              .addLast(new LineBasedFrameDecoder(8192))
              .addLast(new StringDecoder(CharsetUtil.UTF_8))
              .addLast(new StringEncoder(CharsetUtil.UTF_8))
              .addLast(tcpHandler)
          }
        })
      tcpChannel = b.connect(masterHost, masterPort).sync().channel()
      println(s"Netty TCP: 마스터(${masterHost}:${masterPort}) 연결 시도...")
      
      // 2. "Send 스레드" 시작 (Send 큐 -> UDP)
      scheduler.execute(() => runSendLoop())
    
      // 3. "Retry/Timeout 스레드" 시작 (Un-ACKed 맵 스캔)
      // 1초마다 스캔 시작
      scheduler.scheduleAtFixedRate(() => runRetryLoop(), 1, 1, TimeUnit.SECONDS)
      
    } catch {
      case e: Exception => 
        println(s"마스터 연결 실패: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  /** [Private] Send 큐를 처리하는 소비자 스레드 로직 */
  private def runSendLoop(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val (targetId, payload) = state.sendQueue.take()
        
        state.peers.get(targetId) match {
          case Some(address) =>
            if (udpChannel == null || !udpChannel.isActive) {
              println(s"UDP 채널이 비활성 상태라 $targetId 에게 전송 실패")
              // (중요) 전송 실패 시, 큐에 다시 넣어서 재시도
              Thread.sleep(100)
              state.sendQueue.put((targetId, payload))
            } else {
              // [수정 2] 'continue' 오류 수정: 로직을 else 블록으로 이동
              val msgId = s"${myId}-${state.messageIdCounter.getAndIncrement()}"
              val message = s"DATA:$msgId:$payload"
              
              val reliableMsg = ReliableMessage(targetId, payload, msgId, 0, System.currentTimeMillis())
              
              // Un-ACKed 맵에 저장 후 전송
              state.unAckedMessages.put(msgId, reliableMsg)
              
              val buffer = Unpooled.copiedBuffer(message, CharsetUtil.UTF_8)
              val packet = new DatagramPacket(buffer, address)
              udpChannel.writeAndFlush(packet)
            }
            
          case None =>
            // (중요) 피어 정보가 아직 없을 수 있음. 큐에 다시 넣고 대기
            println(s"ID $targetId 주소를 아직 모름. 큐에 재삽입.")
            Thread.sleep(1000) // 1초 대기
            state.sendQueue.put((targetId, payload))
        }
      }
    } catch { case _: InterruptedException => println("Send 스레드 종료.") }
  }
  
  /** [Private] Un-ACKed 맵을 스캔하여 재전송 (장애 감지 X, 무한 재시도) */
  private def runRetryLoop(): Unit = {
    try {
      // (수정) 최대 재시도 횟수(MAX_RETRIES) 제거 -> 부활할 때까지 재시도
      val TIMEOUT_BASE_MS = 1000 // 1초
      val MAX_BACKOFF_POWER = 5  // 최대 2^5 = 32초 간격
      
      val now = System.currentTimeMillis()
      
      state.unAckedMessages.forEach { (msgId, msg) =>
        // 지수적 백오프 (최대 32초)
        val power = Math.min(msg.retryCount, MAX_BACKOFF_POWER)
        val timeoutMs = TIMEOUT_BASE_MS * Math.pow(2, power).toLong
        
        if (now - msg.lastSentTime > timeoutMs) {
          // === 재전송 ===
          msg.retryCount += 1
          msg.lastSentTime = now
            
          state.peers.get(msg.targetId).foreach { address =>
            // (수정) 장애 감지 대신, 부활을 기다리며 계속 재시도
            println(s"재전송(${msg.retryCount}): $msgId -> ${msg.targetId}")
            val fullMessage = s"DATA:${msgId}:${msg.payload}"
            val buffer = Unpooled.copiedBuffer(fullMessage, CharsetUtil.UTF_8)
            val packet = new DatagramPacket(buffer, address)
            udpChannel.writeAndFlush(packet)
          }
        }
      }
    } catch {
      case NonFatal(e) => println(s"Retry 루프 오류: ${e.getMessage}")
    }
  }

  /**
   * (Public) WorkerRuntime이 호출하는 Send 함수.
   * Send 큐에 (targetId, payload)를 넣고 즉시 리턴한다.
   */
  override def send(targetId: Int, message: String): Unit = {
    if (message == null) return
    state.sendQueue.put((targetId, message))
  }
  
  override def stop(): Unit = {
    println("NettyClientService 종료 중...")
    scheduler.shutdownNow() // 모든 스레드(Send/Receive/Retry) 종료
    if (tcpChannel != null) tcpChannel.close().sync()
    if (udpChannel != null) udpChannel.close().sync()
    tcpGroup.shutdownGracefully().sync()
    udpGroup.shutdownGracefully().sync()
  }
  
  // (Public) 마스터로 TCP 전송
  override def send_to_master(message: String): Unit = sendTcpToMaster(message)
}