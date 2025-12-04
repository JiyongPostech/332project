package worker

import common.{Record, Messages}
import network.NetworkService
import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class WorkerRuntime(var id: Int, net: NetworkService, inputDirs: Seq[File], outputDir: File, masterId: Int, masterHost: String, masterPort: Int) {
  
  // [중요] ID가 할당될 때까지 기다리기 위한 동기화 도구
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
        // [중요] 마스터가 보낸 ID 할당 응답 처리
        if (headerStr.startsWith(Messages.TYPE_REGISTER_RESPONSE)) {
           val newId = headerStr.trim.split(Messages.DELIMITER)(1).toInt
           this.id = newId
           println(s"[WorkerRuntime] Assigned ID: $id")
           idLatch.countDown() // 대기 해제 -> sendSamples 실행됨
        }
        else if (headerStr.startsWith(Messages.TYPE_RANGE)) {
           handleKeyRange(payload, headerStr)
        } 
        else if (headerStr.startsWith(Messages.TYPE_ALL_DONE)) {
           handleAllDone()
        }
      } else {
        if (headerStr.startsWith(Messages.TYPE_FIN)) handleFin(senderId)
        else if (payload.length == Record.SIZE) sorter.addRecord(Record(payload))
      }
    }
    
    println(s"[Worker] Connecting to Master ($masterHost:$masterPort)...")
    net.connect(masterHost, masterPort)
    
    // [중요] ID 받을 때까지 여기서 멈춤 (Blocking)
    println("[Worker] Waiting for ID assignment from Master...")
    idLatch.await()
    println(s"[Worker] ID Assigned: $id. Starting pipeline.")

    Thread.sleep(500)
    sendSamples()
  }

  private def sendSamples(): Unit = {
    println(s"[Worker $id] Sampling data...")
    val samples = new ArrayBuffer[Byte]()
    
    // 파일/폴더 구분 처리 (NPE 방지)
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
    println(s"[Worker $id] Starting Shuffle (Streaming)...")
    
    inputDirs.foreach { dir =>
      if(dir.exists()) {
        val files = if (dir.isDirectory) dir.listFiles().filter(_.isFile) else Array(dir)
        files.foreach { file =>
          val iter = FileIO.readRecords(file)
          while (iter.hasNext) {
            val rec = iter.next()
            val targetId = getTargetWorker(rec.key)
            net.send(targetId, rec.toBytes)
          }
        }
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
      // [중요] 최종 결과 파일명에 할당받은 ID 사용
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