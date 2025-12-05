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
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import common.Messages._

class NettyImplementation(var myId: Int, myPort: Int) extends NetworkService {

  private val peers = TrieMap[Int, InetSocketAddress]()
  private val MASTER_ID = 0
  private val workerIdCounter = new AtomicInteger(1)

  // UDP 관련
  private val sendQueue = new LinkedBlockingQueue[(Int, String, Array[Byte])]()
  // (targetId, data, lastSentTime, retryCount)
  private val unAckedMessages = new ConcurrentHashMap[String, (Int, Array[Byte], Long, Int)]()
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
      println(s"[Netty] Worker bound to UDP port $myPort (Waiting for ID assignment...)")
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
        // Loopback: 즉시 처리
        if (appHandler != null) appHandler(myId, data)
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
              if (str.startsWith(TYPE_REGISTER)) handleRegister(ctx, str)
              else {
                 val senderId = getWorkerIdByChannel(ctx.channel())
                 if (senderId != -1 && appHandler != null) appHandler(senderId, bytes)
              }
            }
          })
        }
      })
    b.bind(myPort).sync()
  }

  private def handleRegister(ctx: ChannelHandlerContext, msgStr: String): Unit = {
    val parts = msgStr.trim.split(DELIMITER)
    var reqId = parts(1).toInt
    val udpPort = parts(2).toInt
    val remoteIp = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    
    if (reqId == -1) {
      // [재접속 로직] 기존에 같은 IP로 접속했던 ID가 있는지 확인
      val existingId = peers.find { case (_, addr) => 
        addr.getAddress.getHostAddress == remoteIp 
      }.map(_._1).getOrElse(-1)

      if (existingId != -1) {
        reqId = existingId
        println(s"[Master] Worker re-connected from $remoteIp. Reassigning ID: $reqId")
      } else {
        reqId = workerIdCounter.getAndIncrement()
        println(s"[Master] New Worker connected from $remoteIp. Assigning ID: $reqId")
      }
      
      val response = s"$TYPE_REGISTER_RESPONSE$DELIMITER$reqId"
      sendTcpPacket(ctx.channel(), response.getBytes(CharsetUtil.UTF_8))
    }
    
    workerChannels.put(reqId, ctx.channel())
    val newPeerAddr = new InetSocketAddress(remoteIp, udpPort)
    peers.put(reqId, newPeerAddr)
    
    val joinedMsg = s"$TYPE_PEER_JOINED$DELIMITER$reqId$DELIMITER$remoteIp$DELIMITER$udpPort"
    val joinedBytes = joinedMsg.getBytes(CharsetUtil.UTF_8)
    workerChannels.forEach { (id, c) => if (id != reqId && c.isActive) sendTcpPacket(c, joinedBytes) }

    val sb = new StringBuilder(TYPE_PEER_LIST)
    peers.foreach { case (pid, addr) =>
      sb.append(s"$DELIMITER$pid$DELIMITER${addr.getAddress.getHostAddress}$DELIMITER${addr.getPort}")
    }
    
    sendTcpPacket(ctx.channel(), sb.toString().getBytes(CharsetUtil.UTF_8))
    
    if (appHandler != null) appHandler(reqId, msgStr.getBytes(CharsetUtil.UTF_8))
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
              val str = new String(bytes.take(100), CharsetUtil.UTF_8)
              if (str.startsWith(TYPE_PEER_LIST)) handlePeerList(new String(bytes, CharsetUtil.UTF_8))
              else if (str.startsWith(TYPE_PEER_JOINED)) handlePeerJoined(new String(bytes, CharsetUtil.UTF_8))
              else if (str.startsWith(TYPE_REGISTER_RESPONSE)) {
                val newId = str.trim.split(DELIMITER)(1).toInt
                NettyImplementation.this.myId = newId
                println(s"[Netty] Assigned Worker ID: $newId")
                if (appHandler != null) appHandler(MASTER_ID, bytes)
              }
              else if (appHandler != null) appHandler(MASTER_ID, bytes)
            }
          })
        }
      })
    masterChannel = b.connect(host, port).sync().channel()
    val regMsg = s"$TYPE_REGISTER$DELIMITER-1$DELIMITER$myPort".getBytes(CharsetUtil.UTF_8)
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

  // --- UDP Logic ---
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
    // [1] Sender Thread
    new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        val (targetId, msgId, data) = sendQueue.take()
        if (peers.contains(targetId)) {
           unAckedMessages.put(msgId, (targetId, data, System.currentTimeMillis(), 0))
           sendRealUdp(targetId, TYPE_DATA, msgId, data)
        } else {
           Thread.sleep(500)
           sendQueue.put((targetId, msgId, data))
        }
      }
    }, "UDP-Sender").start()

    // [2] Retry Thread (Base Timeout 5초 + Exponential Backoff)
    new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep(100) 
        val now = System.currentTimeMillis()
        val iter = unAckedMessages.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val msgId = entry.getKey
          val (targetId, data, lastSent, retryCount) = entry.getValue
          
          // [수정] 기본 타임아웃 5초
          val TIMEOUT_BASE = 5000L
          val backoff = Math.min(TIMEOUT_BASE * Math.pow(2, retryCount).toLong, 60000L)
          
          if (now - lastSent > backoff) {
             sendRealUdp(targetId, TYPE_DATA, msgId, data)
             unAckedMessages.put(msgId, (targetId, data, now, retryCount + 1))
          }
        }
      }
    }, "UDP-Retry").start()
  }

  private def sendRealUdp(targetId: Int, typeStr: String, msgId: String, payload: Array[Byte]): Unit = {
    val addr = peers.get(targetId).orNull
    if (addr != null && udpChannel != null) {
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
    val firstColon = str.indexOf(DELIMITER)
    val secondColon = str.indexOf(DELIMITER, firstColon + 1)
    val thirdColon  = str.indexOf(DELIMITER, secondColon + 1)
    
    if (firstColon > 0 && secondColon > 0 && thirdColon > 0) {
      val typeStr = str.substring(0, firstColon)
      val senderIdStr = str.substring(firstColon + 1, secondColon)
      val msgId = str.substring(secondColon + 1, thirdColon)
      val senderId = senderIdStr.toInt
      
      if (typeStr == TYPE_ACK) {
        unAckedMessages.remove(msgId)
      } 
      else if (typeStr == TYPE_DATA || typeStr == TYPE_FIN) {
        // [1] 데이터 처리 (Blocking: 저장될 때까지 대기)
        val headerLen = thirdColon + 1
        val payload = new Array[Byte](bytes.length - headerLen)
        System.arraycopy(bytes, headerLen, payload, 0, payload.length)
        
        if (appHandler != null) {
           appHandler(senderId, payload)
        }

        // [2] 처리가 완료된 후에만 ACK 전송
        val addr = peers.get(senderId).orNull
        if (addr != null || senderId == myId) {
           val header = s"$TYPE_ACK$DELIMITER$myId$DELIMITER$msgId$DELIMITER"
           udpChannel.writeAndFlush(new DatagramPacket(
             Unpooled.copiedBuffer(header, CharsetUtil.UTF_8), sender))
        }
      }
    }
  }
}
