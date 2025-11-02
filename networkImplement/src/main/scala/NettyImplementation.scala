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
}

/** [Netty-Private] 마스터 서버의 Netty 핸들러 */
private class NettyMasterHandler(state: NettyMasterState) extends SimpleChannelInboundHandler[String] {
  override def channelActive(ctx: ChannelHandlerContext): Unit = state.allClients.add(ctx.channel)
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    state.allClients.remove(ctx.channel)
    state.channelToIdMap.remove(ctx.channel).foreach { id =>
      state.peerMap.remove(id)
      state.allClients.writeAndFlush(Codec.encodeMessage(PeerLeft(id)))
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
      case None => println(s"알 수 없는 메시지: $msg")
    }
  }
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = ctx.close()
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
              .addLast(new LineBasedFrameDecoder(1024))
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
      case _ => println(s"마스터로부터 알 수 없는 메시지: $msg")
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
    onPeerLeft: (Int) => Unit
  ): Unit = {
    state.onPeerListCallback = onPeerListReceived
    state.onPeerJoinedCallback = onPeerJoined
    state.onPeerLeftCallback = onPeerLeft
    
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
}
