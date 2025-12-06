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
import common.{Messages, Logger}
import common.Messages._

class NettyImplementation(var myId: Int, myPort: Int) extends NetworkService {

  private val peers = TrieMap[Int, InetSocketAddress]()
  private val MASTER_ID = 0
  private val workerIdCounter = new AtomicInteger(1)

  private val sendQueue = new LinkedBlockingQueue[(Int, String, Array[Byte])]()
  private val unAckedMessages = new ConcurrentHashMap[String, (Int, Array[Byte], Long, Int)]()
  private val receivedMsgIds = java.util.Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  private val udpGroup = new NioEventLoopGroup()
  private var udpChannel: Channel = _

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
      Logger.info(s"[Netty] Master bound to TCP port $myPort")
    } else {
      startUdpServer()
      startBackgroundThreads()
      Logger.info(s"[Netty] Worker bound to UDP port $myPort")
    }
  }

  override def connect(masterHost: String, masterPort: Int): Unit = {
    if (myId != MASTER_ID) {
      try {
        startTcpClient(masterHost, masterPort)
      } catch {
        case e: Exception =>
          Logger.info(s"[Error] Failed to connect to Master at $masterHost:$masterPort. Is Master running?")
          // Allow the caller to handle the exit, or just return. 
          // But throwing it up is better for Launcher to catch.
          throw e 
      }
    }
  }

  override def send(targetId: Int, data: Array[Byte]): Unit = {
    if (myId == MASTER_ID) {
      val ch = workerChannels.get(targetId)
      if (ch != null && ch.isActive) sendTcpPacket(ch, data)
    } else {
      if (targetId == MASTER_ID) {
        // [Fix] Check connection before sending to avoid errors if Master died
        if (masterChannel != null && masterChannel.isActive) {
          sendTcpPacket(masterChannel, data)
        } else {
          Logger.info("[Netty] Cannot send to Master (Connection closed).")
        }
      } else if (targetId == myId) {
        if (appHandler != null) appHandler(myId, data)
      } else {
        val msgId = java.util.UUID.randomUUID().toString
        unAckedMessages.put(msgId, (targetId, data, 0L, 0))
        sendQueue.put((targetId, msgId, data))
      }
    }
  }

  override def stop(): Unit = {
    udpGroup.shutdownGracefully()
    tcpGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }

  override def hasPendingMessages(): Boolean = !sendQueue.isEmpty || !unAckedMessages.isEmpty
  override def getPendingCount(): Int = sendQueue.size() + unAckedMessages.size()

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
      val existingId = peers.find { case (_, addr) => 
        addr.getAddress.getHostAddress == remoteIp 
      }.map(_._1).getOrElse(-1)

      if (existingId != -1) reqId = existingId else reqId = workerIdCounter.getAndIncrement()
      val response = s"$TYPE_REGISTER_RESPONSE$DELIMITER$reqId"
      sendTcpPacket(ctx.channel(), response.getBytes(CharsetUtil.UTF_8))
    }
    workerChannels.put(reqId, ctx.channel())
    peers.put(reqId, new InetSocketAddress(remoteIp, udpPort))
    val joinedMsg = s"$TYPE_PEER_JOINED$DELIMITER$reqId$DELIMITER$remoteIp$DELIMITER$udpPort".getBytes(CharsetUtil.UTF_8)
    workerChannels.forEach { (id, c) => if (id != reqId && c.isActive) sendTcpPacket(c, joinedMsg) }
    val sb = new StringBuilder(TYPE_PEER_LIST)
    peers.foreach { case (pid, addr) => sb.append(s"$DELIMITER$pid$DELIMITER${addr.getAddress.getHostAddress}$DELIMITER${addr.getPort}") }
    sendTcpPacket(ctx.channel(), sb.toString().getBytes(CharsetUtil.UTF_8))
    if (appHandler != null) appHandler(reqId, s"$msgStr$DELIMITER$remoteIp".getBytes(CharsetUtil.UTF_8))
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
                NettyImplementation.this.myId = str.trim.split(DELIMITER)(1).toInt
                if (appHandler != null) appHandler(MASTER_ID, bytes)
              } else if (appHandler != null) appHandler(MASTER_ID, bytes)
            }
          })
        }
      })
    masterChannel = b.connect(host, port).sync().channel()
    sendTcpPacket(masterChannel, s"$TYPE_REGISTER$DELIMITER-1$DELIMITER$myPort".getBytes(CharsetUtil.UTF_8))
  }

  private def handlePeerList(msg: String): Unit = {
    val parts = msg.trim.split(DELIMITER)
    var i = 1
    while (i < parts.length - 2) {
      peers.put(parts(i).toInt, new InetSocketAddress(parts(i+1), parts(i+2).toInt))
      i += 3
    }
  }

  private def handlePeerJoined(msg: String): Unit = {
    val parts = msg.trim.split(DELIMITER)
    peers.put(parts(1).toInt, new InetSocketAddress(parts(2), parts(3).toInt))
    Logger.info(s"Peer ${parts(1)} joined.")
  }

  private def sendTcpPacket(ch: Channel, data: Array[Byte]): Unit = ch.writeAndFlush(Unpooled.copiedBuffer(data))
  
  private def getWorkerIdByChannel(ch: Channel): Int = {
    val iter = workerChannels.entrySet().iterator()
    while(iter.hasNext) { val entry = iter.next(); if (entry.getValue == ch) return entry.getKey }
    -1
  }

  // --- UDP Logic ---
  private def startUdpServer(): Unit = {
    val b = new Bootstrap()
    b.group(udpGroup).channel(classOf[NioDatagramChannel])
      .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(65535))
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
    new Thread(() => {
      try {
        while (!Thread.currentThread().isInterrupted) {
          val (targetId, msgId, data) = sendQueue.take()
          if (peers.contains(targetId)) {
             unAckedMessages.put(msgId, (targetId, data, System.currentTimeMillis(), 0))
             sendRealUdp(targetId, TYPE_DATA, msgId, data)
          } else {
             unAckedMessages.remove(msgId)
             Thread.sleep(500)
             val nextMsgId = java.util.UUID.randomUUID().toString
             unAckedMessages.put(nextMsgId, (targetId, data, 0L, 0))
             sendQueue.put((targetId, nextMsgId, data))
          }
        }
      } catch { case _: Exception => }
    }, "UDP-Sender").start()

    new Thread(() => {
      try {
        while (!Thread.currentThread().isInterrupted) {
          Thread.sleep(50) 
          val now = System.currentTimeMillis()
          val iter = unAckedMessages.entrySet().iterator()
          while (iter.hasNext) {
            val entry = iter.next()
            val msgId = entry.getKey
            val (targetId, data, lastSent, retryCount) = entry.getValue
            
            if (lastSent > 0) {
                val TIMEOUT_BASE = 300L 
                val MAX_BACKOFF = 3000L
                val backoff = Math.min(TIMEOUT_BASE * Math.pow(1.5, retryCount).toLong, MAX_BACKOFF)
                if (now - lastSent > backoff) {
                   sendRealUdp(targetId, TYPE_DATA, msgId, data)
                   unAckedMessages.put(msgId, (targetId, data, now, retryCount + 1))
                }
            }
          }
        }
      } catch { case _: Exception => }
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
    var firstColon = -1
    var secondColon = -1
    var thirdColon = -1
    val LIMIT = Math.min(100, bytes.length)
    var i = 0
    while (i < LIMIT && thirdColon == -1) {
      if (bytes(i) == 58) { 
        if (firstColon == -1) firstColon = i
        else if (secondColon == -1) secondColon = i
        else thirdColon = i
      }
      i += 1
    }

    if (firstColon > 0 && secondColon > 0 && thirdColon > 0) {
      val typeStr = new String(bytes, 0, firstColon, CharsetUtil.UTF_8)
      val senderIdStr = new String(bytes, firstColon + 1, secondColon - firstColon - 1, CharsetUtil.UTF_8)
      val msgId = new String(bytes, secondColon + 1, thirdColon - secondColon - 1, CharsetUtil.UTF_8)
      val senderId = senderIdStr.toInt
      
      if (typeStr == TYPE_ACK) {
        if (unAckedMessages.containsKey(msgId)) {
           unAckedMessages.remove(msgId)
        }
      } 
      else if (typeStr == TYPE_DATA || typeStr == TYPE_FIN) {
        val headerLen = thirdColon + 1
        val payloadLen = bytes.length - headerLen
        
        if (!receivedMsgIds.contains(msgId)) {
            receivedMsgIds.add(msgId)
            val payload = new Array[Byte](payloadLen)
            System.arraycopy(bytes, headerLen, payload, 0, payloadLen)
            if (appHandler != null) appHandler(senderId, payload)
        }

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
