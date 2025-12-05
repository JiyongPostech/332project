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
  
  private var totalPartitions = 0 // [수정] 총 파티션 수 (P)
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
           val newId = headerStr.trim.split(Messages.DELIMITER)(1).toInt
           this.id = newId
           Logger.info(s"Assigned ID: $id")
           idLatch.countDown() 
        }
        else if (headerStr.startsWith(Messages.TYPE_RANGE)) {
           handleKeyRange(payload, headerStr)
        } 
        else if (headerStr.startsWith(Messages.TYPE_ALL_DONE)) {
           handleAllDone()
        }
      } else {
        if (headerStr.startsWith(Messages.TYPE_FIN)) handleFin(senderId)
        else {
          if (payload.length > 0 && payload.length % Record.SIZE == 0) {
             var offset = 0
             while (offset < payload.length) {
                val recBytes = Arrays.copyOfRange(payload, offset, offset + Record.SIZE)
                sorter.addRecord(Record(recBytes))
                offset += Record.SIZE
             }
          }
        }
      }
    }
    
    Logger.info(s"Connecting to Master ($masterHost:$masterPort)...")
    net.connect(masterHost, masterPort)
    
    Logger.info("Waiting for ID assignment...")
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
           while(it.hasNext && count < 1000) {
             samples ++= it.next().key
             count += 1
           }
         }
       }
    }
    
    val header = s"${Messages.TYPE_SAMPLE}${Messages.DELIMITER}"
    val headerBytes = header.getBytes(StandardCharsets.UTF_8)
    val packet = headerBytes ++ samples.toArray
    
    net.send(masterId, packet)
    Logger.info(s"Sent samples to Master.")
  }

  private def handleKeyRange(payload: Array[Byte], headerStr: String): Unit = {
    val delimiterIdx = headerStr.indexOf(Messages.DELIMITER)
    val bodyBytes = payload.drop(delimiterIdx + 1)
    val buf = ByteBuffer.wrap(bodyBytes)
    
    totalPartitions = buf.getInt // 총 파티션 수 (W=P) 수신
    val numPivots = totalPartitions - 1
    
    pivots = new Array[Array[Byte]](numPivots)
    for (i <- 0 until numPivots) {
      val p = new Array[Byte](10)
      buf.get(p)
      pivots(i) = p
    }
    
    Logger.info(s"Received Key Range. Total Partitions: $totalPartitions, Pivots: $numPivots")
    startShuffle()
  }

  private def startShuffle(): Unit = {
    Logger.info("Starting Shuffle...")
    
    val sendBuffers = MutableMap[Int, ArrayBuffer[Byte]]()
    val BATCH_SIZE = 64 * 1024 
    
    inputDirs.foreach { dir =>
      if(dir.exists()) {
        val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
        files.foreach { file =>
          val iter = FileIO.readRecords(file)
          while (iter.hasNext) {
            val rec = iter.next()
            val targetId = getTargetWorker(rec.key)
            
            val buf = sendBuffers.getOrElseUpdate(targetId, new ArrayBuffer[Byte](BATCH_SIZE + Record.SIZE))
            buf ++= rec.toBytes
            
            if (buf.size >= BATCH_SIZE) {
               net.send(targetId, buf.toArray)
               buf.clear()
            }
          }
        }
      }
    }
    
    // 남은 버퍼 모두 전송 (Flush)
    sendBuffers.foreach { case (tid, buf) =>
      if (buf.nonEmpty) {
        net.send(tid, buf.toArray)
        buf.clear()
      }
    }
    
    // FIN 전송: 총 워커 수(2명)에게만 FIN 전송
    val finPacket = s"${Messages.TYPE_FIN}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
    for (pid <- 1 to totalPartitions if pid != id) { // totalPartitions = numWorkers
      net.send(pid, finPacket)
    }
    handleFin(id)
  }
  
  private def getTargetWorker(key: Array[Byte]): Int = {
    for (i <- 0 until pivots.length) {
      if (compareUnsigned(key, pivots(i)) < 0) {
        return i + 1 
      }
    }
    return totalPartitions // Partition ID: W
  }

  private def handleFin(senderId: Int): Unit = synchronized {
    if (finishedPeers.contains(senderId)) return
    
    finishedPeers += senderId
    Logger.info(s"Received FIN from Worker $senderId (${finishedPeers.size}/$totalPartitions)")
    
    if (finishedPeers.size == totalPartitions) { // W명이 모두 FIN을 보냄
      Logger.info("Shuffle complete. Starting Local Merge...")
      sorter.close() 
      
      if (!outputDir.exists()) outputDir.mkdirs()
      // [복구] partition.$id 파일 하나만 생성
      val finalFile = new File(outputDir, s"partition.$id")
      
      merger.merge(sorter.tempFiles.toSeq, finalFile)
      
      Logger.info("Job Finished. Reporting to Master...")
      
      val doneMsg = s"${Messages.TYPE_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
      net.send(masterId, doneMsg)
      
      Logger.info("Waiting for ALL_DONE signal...")
    }
  }
  
  private def handleAllDone(): Unit = {
    Logger.info("Received ALL_DONE. Exiting.")
    net.stop()
    System.exit(0)
  }
}
