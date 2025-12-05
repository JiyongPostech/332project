package worker

import common.{Record, Messages}
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
  
  private var totalPeers = 0
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
           println(s"[WorkerRuntime] Assigned ID: $id")
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
          // 배치(Batch) 데이터 수신 처리
          if (payload.length > 0 && payload.length % Record.SIZE == 0) {
             var offset = 0
             while (offset < payload.length) {
                // 100바이트씩 잘라서 처리
                val recBytes = Arrays.copyOfRange(payload, offset, offset + Record.SIZE)
                sorter.addRecord(Record(recBytes))
                offset += Record.SIZE
             }
          }
        }
      }
    }
    
    println(s"[Worker] Connecting to Master ($masterHost:$masterPort)...")
    net.connect(masterHost, masterPort)
    
    println("[Worker] Waiting for ID assignment from Master...")
    idLatch.await()
    println(s"[Worker] ID Assigned: $id. Starting pipeline.")

    Thread.sleep(500)
    sendSamples()
  }

  private def sendSamples(): Unit = {
    println(s"[Worker $id] Sampling data...")
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
    println(s"[Worker $id] Sent ${samples.size / 10} samples to Master.")
  }

  private def handleKeyRange(payload: Array[Byte], headerStr: String): Unit = {
    val delimiterIdx = headerStr.indexOf(Messages.DELIMITER)
    val bodyBytes = payload.drop(delimiterIdx + 1)
    val buf = ByteBuffer.wrap(bodyBytes)
    
    totalPeers = buf.getInt
    val numPivots = totalPeers - 1
    
    pivots = new Array[Array[Byte]](numPivots)
    for (i <- 0 until numPivots) {
      val p = new Array[Byte](10)
      buf.get(p)
      pivots(i) = p
    }
    
    println(s"[Worker $id] Received Key Range. Total workers: $totalPeers, Pivots: $numPivots")
    startShuffle()
  }

  private def startShuffle(): Unit = {
    println(s"[Worker $id] Starting Shuffle (Batching Enabled)...")
    
    // 각 타겟 워커별 전송 버퍼 (Batching Buffer)
    val sendBuffers = MutableMap[Int, ArrayBuffer[Byte]]()
    val BATCH_SIZE = 64 * 1024 // 64KB 단위로 묶어서 전송
    
    inputDirs.foreach { dir =>
      if(dir.exists()) {
        val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
        files.foreach { file =>
          val iter = FileIO.readRecords(file)
          while (iter.hasNext) {
            val rec = iter.next()
            val targetId = getTargetWorker(rec.key)
            
            // 버퍼에 추가
            val buf = sendBuffers.getOrElseUpdate(targetId, new ArrayBuffer[Byte](BATCH_SIZE + Record.SIZE))
            buf ++= rec.toBytes
            
            // 버퍼가 차면 전송하고 비움
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
    
    val finPacket = s"${Messages.TYPE_FIN}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
    for (pid <- 1 to totalPeers if pid != id) {
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
    return totalPeers
  }

  private def handleFin(senderId: Int): Unit = synchronized {
    if (finishedPeers.contains(senderId)) return
    
    finishedPeers += senderId
    println(s"[Worker $id] Received FIN from Worker $senderId (${finishedPeers.size}/$totalPeers)")
    
    if (finishedPeers.size == totalPeers) {
      println(s"[Worker $id] Shuffle complete. Starting Local Merge...")
      sorter.close() 
      
      if (!outputDir.exists()) outputDir.mkdirs()
      val finalFile = new File(outputDir, s"partition.$id")
      
      merger.merge(sorter.tempFiles.toSeq, finalFile)
      
      println(s"[Worker $id] Job Finished. Reporting to Master...")
      
      val doneMsg = s"${Messages.TYPE_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
      net.send(masterId, doneMsg)
      
      println(s"[Worker $id] Waiting for ALL_DONE signal from Master...")
    }
  }
  
  private def handleAllDone(): Unit = {
    println(s"[Worker $id] Received ALL_DONE. Exiting gracefully.")
    net.stop()
    System.exit(0)
  }
}
