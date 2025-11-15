package network

// src/main/scala/NettyImplementation.scala

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

// ######################################################################
// ##                                                                  ##
// ##               1. Netty 마스터 서버 구현 (Private)                    ##
// ##                                                                  ##
// ######################################################################

/** [Netty-Private] 마스터 서버의 상태 (접속자 목록) */
private class NettyMasterState {
  val allClients: ChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  val peerMap: TrieMap[Int, InetSocketAddress] = TrieMap[Int, InetSocketAddress]()
  val channelToIdMap: TrieMap[Channel, Int] = TrieMap[Channel, Int]()
  // 수집된 분석 결과: workerId -> (min, max, count)
  val analysisResults: TrieMap[Int, (Long, Long, Int)] = TrieMap[Int, (Long, Long, Int)]()
  // 분석 결과와 전역 샘플 버퍼
  val samples: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty[Long]
  // 각 워커로부터 받은 정렬된 데이터 청크
  val sortedChunks: TrieMap[Int, mutable.ArrayBuffer[Long]] = TrieMap[Int, mutable.ArrayBuffer[Long]]()
  
  // 비즈니스 로직 콜백 (MasterCoordinator 연결용)
  var onWorkerRegisteredCallback: Option[common.WorkerInfo => Unit] = None
  var onWorkerLeftCallback: Option[Int => Unit] = None
}

/** [Netty-Private] 마스터 서버의 Netty 핸들러 */
private class NettyMasterHandler(state: NettyMasterState) extends SimpleChannelInboundHandler[String] {
  override def channelActive(ctx: ChannelHandlerContext): Unit = state.allClients.add(ctx.channel)
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    state.allClients.remove(ctx.channel)
    state.channelToIdMap.remove(ctx.channel).foreach { id =>
      state.peerMap.remove(id)
      state.allClients.writeAndFlush(Codec.encodeMessage(PeerLeft(id)))
      // 비즈니스 로직 콜백 호출
      state.onWorkerLeftCallback.foreach(_(id))
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

              // 모든 등록 워커의 분석이 도착했으면 키 범위 계산 후 브로드캐스트
              if (state.peerMap.nonEmpty && state.analysisResults.keySet == state.peerMap.keySet) {
                val globalMin = state.analysisResults.values.map(_._1).min
                val globalMax = state.analysisResults.values.map(_._2).max
                val workers = state.peerMap.keys.toSeq.sorted
                val ranges = computeQuantileRanges(globalMin, globalMax, workers, state.samples.toSeq)
                val msgStr = Codec.encodeMessage(KeyRangeUpdate(ranges))
                state.allClients.writeAndFlush(msgStr)
                state.samples.clear()
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
            
            state.sortedChunks.getOrElseUpdate(workerId, mutable.ArrayBuffer.empty) ++= data
            println(s"마스터: 워커 $workerId 로부터 청크 $chunkIdx 수신 (${data.size}개)")
            
            // 모든 워커의 정렬 데이터가 도착했는지 확인
            if (state.peerMap.nonEmpty && state.sortedChunks.keySet == state.peerMap.keySet) {
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
              
              state.sortedChunks.clear()
            }
          }
        } else {
          println(s"알 수 없는 메시지: $msg")
        }
    }
  }
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = ctx.close()

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
      case e: Exception => stop()
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
// ##              2. Netty 클라이언트 서비스 구현 (Private)                 ##
// ##                                                                  ##
// ######################################################################

/** [Netty-Private] 클라이언트의 TCP (마스터 접속용) 핸들러 */
private class NettyClientTcpHandler(
  myId: Int,
  myUdpPort: Int,
  state: NettyClientState
) extends SimpleChannelInboundHandler[String] {
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.writeAndFlush(Codec.encodeRegister(Register(myId, myUdpPort)))
  }
  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
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

/** [Netty-Private] 클라이언트의 UDP (P2P 통신용) 핸들러 */
private class NettyClientUdpHandler(state: NettyClientState) extends SimpleChannelInboundHandler[DatagramPacket] {
  override def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket): Unit = {
    val message = packet.content().toString(CharsetUtil.UTF_8)
    state.reversePeers.get(packet.sender()).foreach { id =>
      state.onMessageCallback(id, message)
    }
  }
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
  }
}

/** [Netty-Private] 클라이언트의 상태 (피어 목록, 콜백 함수) */
private class NettyClientState {
  val peers: TrieMap[Int, InetSocketAddress] = TrieMap[Int, InetSocketAddress]()
  val reversePeers: TrieMap[InetSocketAddress, Int] = TrieMap[InetSocketAddress, Int]()
  
  var onMessageCallback: (Int, String) => Unit = (_, _) => {}
  var onPeerListCallback: (Map[Int, InetSocketAddress]) => Unit = (_) => {}
  var onPeerJoinedCallback: (Int, InetSocketAddress) => Unit = (_, _) => {}
  var onPeerLeftCallback: (Int) => Unit = (_) => {}
  var onKeyRangeCallback: Seq[(Int, Long, Long)] => Unit = (_: Seq[(Int, Long, Long)]) => {}
  var onInitialDataCallback: String => Unit = (_) => {}  // INITIAL_DATA 처리용
  
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

/**
 * (Public) Netty 클라이언트 서비스 구현체
 * NetworkCommon.scala의 NetworkService 설계도를 구현합니다.
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
  
  // 마스터로의 TCP 전송 헬퍼
  private def sendTcpToMaster(message: String): Unit = {
    if (tcpChannel != null && tcpChannel.isActive) {
      val msg = if (message.endsWith("\n")) message else message + "\n"
      tcpChannel.writeAndFlush(msg)
    } else {
      println("TCP 채널이 활성화되어 있지 않아 마스터 전송 실패")
    }
  }

  override def bind(onMessageReceived: (Int, String) => Unit): Unit = {
    state.onMessageCallback = onMessageReceived
    try {
      val b = new Bootstrap()
      b.group(udpGroup).channel(classOf[NioDatagramChannel])
        .handler(new NettyClientUdpHandler(state))
      udpChannel = b.bind(myUdpPort).sync().channel()
      println(s"Netty UDP: 포트 $myUdpPort 바인딩 성공.")
    } catch {
      case e: Exception => println(s"UDP 바인딩 실패: ${e.getMessage}")
    }
  }

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
    } catch {
      case e: Exception => println(s"마스터 연결 실패: ${e.getMessage}")
    }
  }

  override def send(targetId: Int, message: String): Unit = {
    state.peers.get(targetId) match {
      case Some(address) =>
        if (udpChannel == null || !udpChannel.isActive) return
        val buffer = Unpooled.copiedBuffer(message, CharsetUtil.UTF_8)
        val packet = new DatagramPacket(buffer, address)
        udpChannel.writeAndFlush(packet)
      case None =>
        println(s"ID $targetId 에게 전송 실패: 주소록에 ID가 없습니다.")
    }
  }
  
  override def stop(): Unit = {
    println("NettyClientService 종료 중...")
    if (tcpChannel != null) tcpChannel.close().sync()
    if (udpChannel != null) udpChannel.close().sync()
    tcpGroup.shutdownGracefully().sync()
    udpGroup.shutdownGracefully().sync()
  }
  
  // NetworkService 확장: 마스터로의 TCP 전송
  def send_to_master(message: String): Unit = sendTcpToMaster(message)
}
