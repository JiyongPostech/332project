package master

import common.{Messages, Record, Logger}
import network.NetworkService
import java.nio.charset.StandardCharsets
import java.util.Arrays
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

class MasterCoordinator(net: NetworkService, numWorkers: Int) { 
  
  // [복구] 총 파티션 수는 워커 수와 동일하게 설정 (1 Worker = 1 Partition)
  private val TOTAL_PARTITIONS = numWorkers 

  private val registeredWorkers = MutableMap[Int, String]() 
  private val samples = new ArrayBuffer[Array[Byte]]()
  private val doneWorkers = new ArrayBuffer[Int]()
  
  Logger.info(s"Coordinator started. Expecting $numWorkers workers. TOTAL_PARTITIONS: $TOTAL_PARTITIONS")

  def handleMessage(senderId: Int, payload: Array[Byte]): Unit = {
    val str = new String(payload, StandardCharsets.UTF_8)
    
    if (str.startsWith(Messages.TYPE_REGISTER)) {
       val parts = str.split(Messages.DELIMITER)
       val workerIp = if (parts.length >= 4) parts(3) else "unknown"

       if (!registeredWorkers.contains(senderId)) {
         registeredWorkers(senderId) = workerIp
         Logger.info(s"Worker $senderId registered ($workerIp)")
         
         if (registeredWorkers.size == numWorkers) {
           val sortedIps = registeredWorkers.toSeq.sortBy(_._1).map(_._2).mkString(", ")
           println(sortedIps)
           Logger.info(s"All workers registered: $sortedIps")
         }
       }
    }
    else if (str.startsWith(Messages.TYPE_SAMPLE)) {
       val dataPart = payload.drop(str.indexOf(Messages.DELIMITER) + 1)
       samples.appendAll(dataPart.grouped(Record.SIZE / 10).map(_.take(10)))
       
       Logger.info(s"Received samples from Worker $senderId (${samples.size} keys total)")
       
       if (registeredWorkers.size == numWorkers && samples.size >= numWorkers * 10) { 
          distributeKeyRanges()
          samples.clear()
       }
    }
    else if (str.startsWith(Messages.TYPE_DONE)) {
       if (!doneWorkers.contains(senderId)) {
         doneWorkers += senderId
         Logger.info(s"Worker $senderId finished (${doneWorkers.size}/$numWorkers)")
         
         if (doneWorkers.size == numWorkers) {
           Logger.info("All tasks completed. Sending ALL_DONE.")
           val allDone = s"${Messages.TYPE_ALL_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
           registeredWorkers.keys.foreach { wid =>
             net.send(wid, allDone)
           }
           Logger.info("Job completed successfully.")
           System.exit(0)
         }
       }
    }
  }

  private def distributeKeyRanges(): Unit = {
    Logger.info(s"Computing $TOTAL_PARTITIONS key ranges...")
    
    val sortedSamples = samples.sortWith(compareUnsigned(_, _) < 0)
    
    // 피벗은 TOTAL_PARTITIONS 기준으로 (N-1개)
    val numPivots = TOTAL_PARTITIONS - 1 
    val pivots = new ArrayBuffer[Byte]()
    
    val step = sortedSamples.size / TOTAL_PARTITIONS
    for (i <- 1 until TOTAL_PARTITIONS) {
      val pivotIndex = i * step
      if (pivotIndex < sortedSamples.size) {
        pivots ++= sortedSamples(pivotIndex)
      }
    }
    
    val header = s"${Messages.TYPE_RANGE}${Messages.DELIMITER}"
    val headerBytes = header.getBytes(StandardCharsets.UTF_8)
    
    // Body: TOTAL_PARTITIONS (4byte) + Pivots
    val bodyBuf = java.nio.ByteBuffer.allocate(4 + pivots.size)
    bodyBuf.putInt(TOTAL_PARTITIONS) 
    bodyBuf.put(pivots.toArray)
    
    val packet = headerBytes ++ bodyBuf.array()
    
    registeredWorkers.keys.foreach { wid =>
      net.send(wid, packet)
    }
    Logger.info(s"Broadcasted $TOTAL_PARTITIONS ranges.")
  }
  
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
}
