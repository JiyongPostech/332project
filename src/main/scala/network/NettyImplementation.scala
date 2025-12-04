package network

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.{DatagramPacket, SocketChannel}
import io.netty.channel.socket.nio.{NioDatagramChannel, NioServerSocketChannel, NioSocketChannel}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import io.netty.util.CharsetUtil

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import scala.collection.concurrent.TrieMap
import common.Messages._

class NettyImplementation(myId: Int, myPort: Int) extends NetworkService {

  private val peers = TrieMap[Int, InetSocketAddress]()
  private val MASTER_ID = 0

  // UDP 관련
  private val sendQueue = new LinkedBlockingQueue[(Int, String, Array[Byte])]()
  private val unAckedMessages = new ConcurrentHashMap[String, (Int, Array[Byte], Long)]()
  private val udpGroup = new NioEventLoopGroup()
  private var udpChannel: Channel = _

  // TCP 관련
  private val tcpGroup = new NioEventLoopGroup()
  private val bossGroup = new NioEventLoopGroup()
  private var tcpChannel: Channel = _             
  private val workerChannels = new ConcurrentHashMap[Int, Channel]() 
  private var masterChannel: Channel = _

  private var appHandler: (Int, Array[Byte]) => Unit = _

  override def bind(handler: (Int, Array[Byte]) => Unit): Unit = {
    this.appHandler = handler
    
    if (myId == MASTER_ID) {
      startTcpServer()
      println(s"[Netty] Master bound to TCP port $myPort")
    } else {
      startUdpServer()
      startBackgroundThreads()
      println(s"[Netty] Worker $myId bound to UDP port $myPort")
    }
  }

  override def connect(masterHost: String, masterPort: Int): Unit = {
    if (myId != MASTER_ID) startTcpClient(masterHost, masterPort)
  }

  override def send(targetId: Int, data: Array[Byte]): Unit = {
    if (myId == MASTER_ID) {
      val ch = workerChannels.get(targetId)
      if (ch != null && ch.isActive) sendTcpPacket(ch, data)
    } else {
      if (targetId == MASTER_ID) {
        if (masterChannel != null && masterChannel.isActive) sendTcpPacket(masterChannel, data)
      } else if (targetId == myId) {
        // [수정] 자기 자신에게 보내는 데이터는 네트워크 안 타고 즉시 처리 (Loopback)
        if (appHandler != null) {
          // 비동기 처리를 위해 스레드 풀에 넘기거나 여기서 바로 호출
          // 순서 보장을 위해 바로 호출함 (또는 큐를 경유해도 됨)
          appHandler(myId, data)
        }
      } else {
        val msgId = java.util.UUID.randomUUID().toString
        sendQueue.put((targetId, msgId, data))
      }
    }
  }

  override def stop(): Unit = {
    udpGroup.shutdownGracefully()
    tcpGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }

  // --- TCP Logic ---

  private def startTcpServer(): Unit = {
    val b = new ServerBootstrap()
    b.group(bossGroup, tcpGroup).channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(10485760, 0, 4, 0, 4))
          ch.pipeline().addLast(new LengthFieldPrepender(4))
          ch.pipeline().addLast(new SimpleChannelInboundHandler[ByteBuf] {
            override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
              val bytes = new Array[Byte](msg.readableBytes())
              msg.readBytes(bytes)
              
              val str = new String(bytes.take(50), CharsetUtil.UTF_8)
              if (str.startsWith(TYPE_REGISTER)) {
                 handleRegister(ctx, str)
              } else {
                 val senderId = getWorkerIdByChannel(ctx.channel())
                 if (appHandler != null) appHandler(senderId, bytes)
              }
            }
          })
        }
      })
    b.bind(myPort).sync()
  }

  private def handleRegister(ctx: ChannelHandlerContext, msgStr: String): Unit = {
    val parts = msgStr.trim.split(DELIMITER)
    val workerId = parts(1).toInt
    val udpPort = parts(2).toInt
    val remoteIp = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    
    workerChannels.put(workerId, ctx.channel())
    val newPeerAddr = new InetSocketAddress(remoteIp, udpPort)
    
    val joinedMsg = s"$TYPE_PEER_JOINED$DELIMITER$workerId$DELIMITER$remoteIp$DELIMITER$udpPort"
    val joinedBytes = joinedMsg.getBytes(CharsetUtil.UTF_8)
    workerChannels.forEach { (id, c) => if (id != workerId && c.isActive) sendTcpPacket(c, joinedBytes) }

    val sb = new StringBuilder(TYPE_PEER_LIST)
    peers.foreach { case (pid, addr) =>
      sb.append(s"$DELIMITER$pid$DELIMITER${addr.getAddress.getHostAddress}$DELIMITER${addr.getPort}")
    }
    peers.put(workerId, newPeerAddr)
    
    sendTcpPacket(ctx.channel(), sb.toString().getBytes(CharsetUtil.UTF_8))
    println(s"[Master] Worker $workerId registered ($remoteIp:$udpPort).")
    
    if (appHandler != null) appHandler(workerId, msgStr.getBytes(CharsetUtil.UTF_8))
  }

  private def startTcpClient(host: String, port: Int): Unit = {
    val b = new Bootstrap()
    b.group(tcpGroup).channel(classOf[NioSocketChannel])
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(10485760, 0, 4, 0, 4))
          ch.pipeline().addLast(new LengthFieldPrepender(4))
          ch.pipeline().addLast(new SimpleChannelInboundHandler[ByteBuf] {
            override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf): Unit = {
              val bytes = new Array[Byte](msg.readableBytes())
              msg.readBytes(bytes)
              val str = new String(bytes.take(50), CharsetUtil.UTF_8)
              if (str.startsWith(TYPE_PEER_LIST)) handlePeerList(new String(bytes, CharsetUtil.UTF_8))
              else if (str.startsWith(TYPE_PEER_JOINED)) handlePeerJoined(new String(bytes, CharsetUtil.UTF_8))
              else if (appHandler != null) appHandler(MASTER_ID, bytes)
            }
          })
        }
      })
    masterChannel = b.connect(host, port).sync().channel()
    val regMsg = s"$TYPE_REGISTER$DELIMITER$myId$DELIMITER$myPort".getBytes(CharsetUtil.UTF_8)
    sendTcpPacket(masterChannel, regMsg)
  }

  private def handlePeerList(msg: String): Unit = {
    val parts = msg.trim.split(DELIMITER)
    var i = 1
    while (i < parts.length - 2) {
      val pid = parts(i).toInt
      val pip = parts(i+1)
      val pport = parts(i+2).toInt
      peers.put(pid, new InetSocketAddress(pip, pport))
      i += 3
    }
  }

  private def handlePeerJoined(msg: String): Unit = {
    val parts = msg.trim.split(DELIMITER)
    val pid = parts(1).toInt
    val pip = parts(2)
    val pport = parts(3).toInt
    peers.put(pid, new InetSocketAddress(pip, pport))
    println(s"[Netty] Peer $pid joined.")
  }

  private def sendTcpPacket(ch: Channel, data: Array[Byte]): Unit = {
    ch.writeAndFlush(Unpooled.copiedBuffer(data))
  }
  private def getWorkerIdByChannel(ch: Channel): Int = {
    val iter = workerChannels.entrySet().iterator()
    while(iter.hasNext) {
      val entry = iter.next()
      if (entry.getValue == ch) return entry.getKey
    }
    -1
  }

  // --- UDP Logic (Reliable) ---

  private def startUdpServer(): Unit = {
    val b = new Bootstrap()
    b.group(udpGroup).channel(classOf[NioDatagramChannel])
      .handler(new SimpleChannelInboundHandler[DatagramPacket] {
        override def channelRead0(ctx: ChannelHandlerContext, packet: DatagramPacket): Unit = {
          val content = packet.content()
          val bytes = new Array[Byte](content.readableBytes())
          content.readBytes(bytes)
          handleUdpPacket(packet.sender(), bytes)
        }
      })
    udpChannel = b.bind(myPort).sync().channel()
  }

  private def startBackgroundThreads(): Unit = {
    // Sender
    new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        val (targetId, msgId, data) = sendQueue.take()
        // 피어 정보 확인 후 전송 (없으면 잠시 대기 후 재시도)
        if (peers.contains(targetId)) {
           unAckedMessages.put(msgId, (targetId, data, System.currentTimeMillis()))
           sendRealUdp(targetId, TYPE_DATA, msgId, data)
        } else {
           Thread.sleep(500)
           sendQueue.put((targetId, msgId, data))
        }
      }
    }, "UDP-Sender").start()

    // Retry
    new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep(1000)
        val now = System.currentTimeMillis()
        val iter = unAckedMessages.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val (targetId, data, lastSent) = entry.getValue
          if (now - lastSent > 1000) {
             sendRealUdp(targetId, TYPE_DATA, entry.getKey, data)
             unAckedMessages.put(entry.getKey, (targetId, data, now))
          }
        }
      }
    }, "UDP-Retry").start()
  }

  private def sendRealUdp(targetId: Int, typeStr: String, msgId: String, payload: Array[Byte]): Unit = {
    val addr = peers.get(targetId).orNull
    if (addr != null && udpChannel != null) {
      // [수정] 헤더에 내 ID(myId) 포함: "TYPE:SENDER_ID:MSG_ID:"
      val header = s"$typeStr$DELIMITER$myId$DELIMITER$msgId$DELIMITER"
      val headerBytes = header.getBytes(CharsetUtil.UTF_8)
      val buffer = Unpooled.buffer(headerBytes.length + payload.length)
      buffer.writeBytes(headerBytes)
      buffer.writeBytes(payload)
      udpChannel.writeAndFlush(new DatagramPacket(buffer, addr))
    }
  }

  private def handleUdpPacket(sender: InetSocketAddress, bytes: Array[Byte]): Unit = {
    val str = new String(bytes.take(100), CharsetUtil.UTF_8)
    // 파싱: TYPE:SENDER:MSGID:PAYLOAD
    val firstColon = str.indexOf(DELIMITER)
    val secondColon = str.indexOf(DELIMITER, firstColon + 1)
    val thirdColon  = str.indexOf(DELIMITER, secondColon + 1)
    
    if (firstColon > 0 && secondColon > 0 && thirdColon > 0) {
      val typeStr = str.substring(0, firstColon)
      val senderIdStr = str.substring(firstColon + 1, secondColon)
      val msgId = str.substring(secondColon + 1, thirdColon)
      val senderId = senderIdStr.toInt // [수정] 헤더에서 직접 ID 추출
      
      if (typeStr == TYPE_ACK) {
        unAckedMessages.remove(msgId)
      } else if (typeStr == TYPE_DATA || typeStr == TYPE_FIN) {
        // 1. ACK 답신 (내 ID를 넣어서 보냄)
        val addr = peers.get(senderId).orNull
        if (addr != null || senderId == myId) { // addr check
           // ACK 보낼 땐 Payload 없이 헤더만
           // ACK:MY_ID:MSG_ID:
           val header = s"$TYPE_ACK$DELIMITER$myId$DELIMITER$msgId$DELIMITER"
           udpChannel.writeAndFlush(new DatagramPacket(
             Unpooled.copiedBuffer(header, CharsetUtil.UTF_8), sender))
        }

        // 2. 앱 전달
        val headerLen = thirdColon + 1
        val payload = new Array[Byte](bytes.length - headerLen)
        System.arraycopy(bytes, headerLen, payload, 0, payload.length)
        
        if (appHandler != null) appHandler(senderId, payload)
      }
    }
  }
}