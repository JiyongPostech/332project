package master

import network.NetworkService
import common.Messages
import java.nio.charset.StandardCharsets
import java.util.Arrays
import scala.collection.mutable

class MasterCoordinator(net: NetworkService, expectedWorkers: Int) {
  private val connectedWorkers = mutable.Set[Int]()
  private val doneWorkers = mutable.Set[Int]()
  
  // 샘플링 관련 상태
  private val samples = mutable.ArrayBuffer[Array[Byte]]() 
  private val sampledWorkers = mutable.Set[Int]()

  // Java 8 호환 Unsigned Byte 비교 함수
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
    println(s"[Master] Started. Expecting $expectedWorkers workers.")
    
    net.bind { (senderId, payload) =>
      // 안전한 헤더 파싱 (길이 체크)
      val headerStr = if (payload.length >= 50) new String(payload.take(50), StandardCharsets.UTF_8) else new String(payload, StandardCharsets.UTF_8)
      
      if (headerStr.contains(Messages.TYPE_REGISTER)) {
        synchronized {
          connectedWorkers.add(senderId)
          if (connectedWorkers.size == expectedWorkers) {
            println(s"[Master] All $expectedWorkers workers registered.")
          }
        }
      }
      else if (headerStr.startsWith(Messages.TYPE_SAMPLE)) {
        // [1] 샘플 수신
        val delimiterIdx = headerStr.indexOf(Messages.DELIMITER)
        if (delimiterIdx > 0) {
          val headerLen = delimiterIdx + 1
          val dataLen = payload.length - headerLen
          
          // 10바이트씩 잘라서 저장
          if (dataLen > 0 && dataLen % 10 == 0) {
             val data = payload.slice(headerLen, payload.length)
             synchronized {
               for (i <- 0 until dataLen / 10) {
                 samples += data.slice(i * 10, (i + 1) * 10)
               }
               sampledWorkers.add(senderId)
               println(s"[Master] Received samples from Worker $senderId (${sampledWorkers.size}/$expectedWorkers)")
               
               // [2] 모든 워커의 샘플이 도착했는지 확인 (Deadlock 방지)
               if (sampledWorkers.size == expectedWorkers) {
                 distributeRanges()
               }
             }
          }
        }
      }
      else if (headerStr.startsWith(Messages.TYPE_DONE)) {
        // [3] 완료 보고 수신
        synchronized {
          doneWorkers.add(senderId)
          println(s"[Master] Worker $senderId finished. (${doneWorkers.size}/$expectedWorkers)")
          
          // [4] 모든 워커 완료 시 해산 명령 (ALL_DONE)
          if (doneWorkers.size == expectedWorkers) {
            println("[Master] All tasks completed. Broadcasting ALL_DONE...")
            val allDoneMsg = s"${Messages.TYPE_ALL_DONE}${Messages.DELIMITER}".getBytes(StandardCharsets.UTF_8)
            
            for (wid <- 1 to expectedWorkers) {
               net.send(wid, allDoneMsg)
            }
            
            // 잠시 대기 후 마스터 종료
            new Thread(() => {
              Thread.sleep(2000) 
              println("[Master] Shutdown.")
              net.stop()
              System.exit(0)
            }).start()
          }
        }
      }
    }
  }

  private var rangesDistributed = false
  
  private def distributeRanges(): Unit = synchronized {
    if (rangesDistributed) return
    rangesDistributed = true
    
    println(s"[Master] Computing key ranges from ${samples.size} samples...")
    
    // 1. 샘플 정렬
    val sortedSamples = samples.sortWith { (a, b) => 
      compareUnsigned(a, b) < 0 
    }
    
    // 2. 피벗(Pivot) 계산
    val numPivots = expectedWorkers - 1
    val pivots = new mutable.ArrayBuffer[Byte]()
    
    if (sortedSamples.nonEmpty && numPivots > 0) {
      val step = sortedSamples.size / expectedWorkers
      for (i <- 1 to numPivots) {
        val pivotIndex = i * step
        if (pivotIndex < sortedSamples.size) {
          pivots ++= sortedSamples(pivotIndex)
        }
      }
    }
    
    // 3. 메시지 생성 및 전송
    val header = s"${Messages.TYPE_RANGE}${Messages.DELIMITER}"
    val headerBytes = header.getBytes(StandardCharsets.UTF_8)
    
    val bodyBuf = java.nio.ByteBuffer.allocate(4 + pivots.size)
    bodyBuf.putInt(expectedWorkers)
    bodyBuf.put(pivots.toArray)
    
    val packet = headerBytes ++ bodyBuf.array()
    
    for (wid <- 1 to expectedWorkers) {
      net.send(wid, packet)
    }
    println(s"[Master] Broadcasted ranges (Pivots: $numPivots).")
  }
}