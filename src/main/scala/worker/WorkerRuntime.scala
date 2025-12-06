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
  
  // [안전장치] 시스템 임시 경로 하위에 '워커 전용 폴더'를 따로 생성 (격리)
  // 예: /tmp/332project-worker-123e4567-e89b...
  private val sysTmp = System.getProperty("java.io.tmpdir")
  private val tmpDir = new File(sysTmp, s"332project-worker-${java.util.UUID.randomUUID()}")
  
  private val sorter = new DataSorter(tmpDir)
  private val merger = new DiskMerger()
  private var finishedPeers = Set[Int]()
  
  @volatile private var totalPartitions = 0 
  @volatile private var pivots: Array[Array[Byte]] = _ 

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
    // 워커 전용 임시 폴더 생성
    if (!tmpDir.exists()) tmpDir.mkdirs()
    Logger.info(s"Using temporary directory: ${tmpDir.getAbsolutePath}")
    
    sorter.start()
    net.bind { (senderId, payload) =>
      val headerPreview = if (payload.length >= 50) new String(payload.take(50), StandardCharsets.UTF_8) else new String(payload, StandardCharsets.UTF_8)
      
      if (senderId == masterId) {
        if (headerPreview.startsWith(Messages.TYPE_REGISTER_RESPONSE)) {
           val str = new String(payload, StandardCharsets.UTF_8)
           this.id = str.trim.split(Messages.DELIMITER)(1).toInt
           Logger.info(s"Assigned ID: $id")
           idLatch.countDown() 
        } 
        else if (headerPreview.startsWith(Messages.TYPE_RANGE)) handleKeyRange(payload)
        else if (headerPreview.startsWith(Messages.TYPE_ALL_DONE)) handleAllDone()
      } else {
        if (headerPreview.startsWith(Messages.TYPE_FIN)) handleFin(senderId)
        else {
          if (payload.length > 0 && payload.length % Record.SIZE == 0) {
             var offset = 0
             while (offset < payload.length) {
                sorter.addRecord(Record(Arrays.copyOfRange(payload, offset, offset + Record.SIZE)))
                offset += Record.SIZE
             }
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

  private def handleKeyRange(payload: Array[Byte]): Unit = {
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
    Logger.info(s"Received Key Range. P=$totalPartitions. Starting Shuffle Thread.")
    new Thread(() => startShuffle()).start()
  }

  private def startShuffle(): Unit = {
    Logger.info("Starting Shuffle (Async)...")
    val sendBuffers = MutableMap[Int, ArrayBuffer[Byte]]()
    val BATCH_SIZE = 30 * 1024 
    val MAX_IN_FLIGHT = 50 

    inputDirs.foreach { dir =>
      if(dir.exists()) {
        val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
        files.foreach { file =>
          Logger.info(s"Reading: ${file.getName}")
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
    
    Logger.info("Waiting for ACKs...")
    while (net.hasPendingMessages()) {
      Thread.sleep(100)
    }
    Logger.info("Shuffle Phase 1 Complete. Sending FIN.")
    
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
      
      // [안전 삭제] 내가 만든 그 전용 폴더만 삭제
      Logger.info(s"Cleaning up temporary directory: ${tmpDir.getAbsolutePath}")
      deleteRecursively(tmpDir)
      
      Logger.info("Job Finished. Reporting to Master...")
      net.send(masterId, s"${Messages.TYPE_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8))
      Logger.info("Waiting for ALL_DONE...")
    }
  }
  
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) files.foreach(deleteRecursively)
    }
    file.delete()
  }
  
  private def handleAllDone(): Unit = {
    Logger.info("Received ALL_DONE. Exiting.")
    net.stop()
    System.exit(0)
  }
}
