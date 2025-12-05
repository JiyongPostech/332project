package worker

import common.{Record, Messages, Logger}
import network.NetworkService
import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Arrays
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

class WorkerRuntime(var id: Int, net: NetworkService, inputDirs: Seq[File], outputDir: File, masterId: Int, masterHost: String, masterPort: Int) {
  
  private val idLatch = new java.util.concurrent.CountDownLatch(1)
  private val sorter = new DataSorter(new File(outputDir, "tmp"))
  private val merger = new DiskMerger()
  private var finishedPeers = Set[Int]()
  private var totalPartitions = 0 
  private var pivots: Array[Array[Byte]] = _ 

  private def compareUnsigned(a: Array[Byte], b: Array[Byte]): Int = {
    val len = Math.min(a.length, b.length)
    var i = 0
    while (i < len) {
      val v1 = a(i) & 0xFF
      val v2 = b(i) & 0xFF
      if (v1 != v2) return v1 - v2
      i += 1
    }
    a.length - b.length
  }

  def start(): Unit = {
    sorter.start()
    net.bind { (senderId, payload) =>
      val headerStr = if (payload.length >= 50) new String(payload.take(50), StandardCharsets.UTF_8) else new String(payload, StandardCharsets.UTF_8)
      if (senderId == masterId) {
        if (headerStr.startsWith(Messages.TYPE_REGISTER_RESPONSE)) {
           this.id = headerStr.trim.split(Messages.DELIMITER)(1).toInt
           Logger.info(s"Assigned ID: $id")
           idLatch.countDown() 
        } else if (headerStr.startsWith(Messages.TYPE_RANGE)) handleKeyRange(payload, headerStr)
        else if (headerStr.startsWith(Messages.TYPE_ALL_DONE)) handleAllDone()
      } else {
        if (headerStr.startsWith(Messages.TYPE_FIN)) handleFin(senderId)
        else {
          if (payload.length > 0 && payload.length % Record.SIZE == 0) {
             var offset = 0
             while (offset < payload.length) {
                sorter.addRecord(Record(Arrays.copyOfRange(payload, offset, offset + Record.SIZE)))
                offset += Record.SIZE
             }
          } else {
             // println 제거됨 (필요시 Logger 사용)
          }
        }
      }
    }
    Logger.info(s"Connecting to Master ($masterHost:$masterPort)...")
    net.connect(masterHost, masterPort)
    idLatch.await()
    Logger.info(s"ID $id assigned. Starting pipeline.")
    Thread.sleep(500)
    sendSamples()
  }

  private def sendSamples(): Unit = {
    Logger.info("Sampling data...")
    val samples = new ArrayBuffer[Byte]()
    inputDirs.foreach { dir =>
       if(dir.exists()) {
         val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
         files.foreach { f =>
           val it = FileIO.readRecords(f)
           var count = 0
           while(it.hasNext && count < 1000) { samples ++= it.next().key; count += 1 }
         }
       }
    }
    val header = s"${Messages.TYPE_SAMPLE}${Messages.DELIMITER}"
    val packet = header.getBytes(StandardCharsets.UTF_8) ++ samples.toArray
    net.send(masterId, packet)
    Logger.info(s"Sent samples to Master.")
  }

  private def handleKeyRange(payload: Array[Byte], headerStr: String): Unit = {
    // 바이트 파싱
    var delimiterIdx = -1
    var i = 0
    while (i < payload.length && delimiterIdx == -1) {
      if (payload(i) == ':'.toByte) delimiterIdx = i
      i += 1
    }
    
    val bodyBytes = new Array[Byte](payload.length - delimiterIdx - 1)
    System.arraycopy(payload, delimiterIdx + 1, bodyBytes, 0, bodyBytes.length)
    val buf = ByteBuffer.wrap(bodyBytes)
    
    totalPartitions = buf.getInt 
    val numPivots = totalPartitions - 1
    pivots = new Array[Array[Byte]](numPivots)
    for (i <- 0 until numPivots) { val p = new Array[Byte](10); buf.get(p); pivots(i) = p }
    Logger.info(s"Received Key Range. P=$totalPartitions")
    
    // 셔플은 별도 스레드에서 실행
    new Thread(() => startShuffle()).start()
  }

  private def startShuffle(): Unit = {
    Logger.info("Starting Shuffle...")
    val sendBuffers = MutableMap[Int, ArrayBuffer[Byte]]()
    val BATCH_SIZE = 30 * 1024 
    val MAX_IN_FLIGHT = 50 

    inputDirs.foreach { dir =>
      if(dir.exists()) {
        val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
        files.foreach { file =>
          Logger.info(s"Reading file: ${file.getAbsolutePath}")
          val iter = FileIO.readRecords(file)
          while (iter.hasNext) {
            val rec = iter.next()
            val targetId = getTargetWorker(rec.key)
            val buf = sendBuffers.getOrElseUpdate(targetId, new ArrayBuffer[Byte](BATCH_SIZE + Record.SIZE))
            buf ++= rec.toBytes
            
            if (buf.size >= BATCH_SIZE) {
               while (net.getPendingCount() > MAX_IN_FLIGHT) Thread.sleep(5)
               net.send(targetId, buf.toArray)
               buf.clear()
            }
          }
        }
      }
    }

    sendBuffers.foreach { case (tid, buf) =>
      if (buf.nonEmpty) {
        while (net.getPendingCount() > MAX_IN_FLIGHT) Thread.sleep(5)
        net.send(tid, buf.toArray)
        buf.clear()
      }
    }
    
    Logger.info("Waiting for all data packets to be ACKed...")
    while (net.hasPendingMessages()) {
      Thread.sleep(100)
    }
    Logger.info("All data packets ACKed. Sending FIN.")
    
    val finPacket = s"${Messages.TYPE_FIN}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
    for (pid <- 1 to totalPartitions if pid != id) { net.send(pid, finPacket) }
    handleFin(id)
  }
  
  private def getTargetWorker(key: Array[Byte]): Int = {
    for (i <- 0 until pivots.length) {
      if (compareUnsigned(key, pivots(i)) < 0) return i + 1 
    }
    totalPartitions 
  }

  private def handleFin(senderId: Int): Unit = synchronized {
    if (finishedPeers.contains(senderId)) return
    finishedPeers += senderId
    Logger.info(s"Received FIN from Worker $senderId (${finishedPeers.size}/$totalPartitions)")
    
    if (finishedPeers.size == totalPartitions) { 
      Logger.info("Shuffle complete. Starting Local Merge...")
      sorter.close() 
      if (!outputDir.exists()) outputDir.mkdirs()
      val finalFile = new File(outputDir, s"partition.$id")
      merger.merge(sorter.tempFiles.toSeq, finalFile)
      Logger.info("Job Finished. Reporting to Master...")
      net.send(masterId, s"${Messages.TYPE_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8))
      Logger.info("Waiting for ALL_DONE signal...")
    }
  }
  
  private def handleAllDone(): Unit = {
    Logger.info("Received ALL_DONE. Exiting.")
    net.stop()
    System.exit(0)
  }
}
