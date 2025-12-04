package worker

import common.Record
import java.io.{File, PrintWriter}
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

class DataSorter(tempDir: File) {
  // [수정 핵심] 큐 크기를 5로 제한 (무제한 -> 5)
  // 이렇게 하면 디스크 쓰기가 느릴 때, 큐가 꽉 차서 addRecord가 잠시 멈추고(Block),
  // 결과적으로 네트워크 수신 속도가 조절되어 메모리가 터지지 않습니다.
  private val flushQueue = new LinkedBlockingQueue[ArrayBuffer[Record]](5)
  
  private val MEMORY_LIMIT = 100 * 1024 * 1024 // 100MB 버퍼
  private var activeBuffer = new ArrayBuffer[Record]()
  private var currentSize = 0
  
  val tempFiles = new ArrayBuffer[File]()
  
  // 디스크 쓰기 스레드
  private val flushThread = new Thread(() => {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val buffer = flushQueue.take() // 데이터 꺼내기
        if (buffer.isEmpty) return // 종료 신호
        
        flushToDisk(buffer)
      }
    } catch {
      case e: InterruptedException => // 종료
    }
  })

  def start(): Unit = {
    if (!tempDir.exists()) tempDir.mkdirs()
    flushThread.start()
  }

  def addRecord(record: Record): Unit = synchronized {
    activeBuffer += record
    currentSize += Record.SIZE
    
    if (currentSize >= MEMORY_LIMIT) {
      // 버퍼가 차면 큐에 넣음 (큐가 꽉 차있으면 여기서 대기함 -> Backpressure 작동!)
      flushQueue.put(activeBuffer)
      activeBuffer = new ArrayBuffer[Record]()
      currentSize = 0
    }
  }

  def close(): Unit = {
    synchronized {
      if (activeBuffer.nonEmpty) {
        flushQueue.put(activeBuffer)
      }
    }
    // 종료 신호 (빈 버퍼)
    flushQueue.put(new ArrayBuffer[Record]())
    try {
      flushThread.join()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
  }

  private def flushToDisk(buffer: ArrayBuffer[Record]): Unit = {
    // 메모리 내 정렬 (Key 기준)
    // Record는 바이트 배열이므로 커스텀 정렬 필요
    val sorted = buffer.sortWith((a, b) => compare(a.key, b.key) < 0)
    
    val file = new File(tempDir, s"segment_${tempFiles.size}.dat")
    FileIO.writeRecords(file, sorted)
    
    synchronized {
      tempFiles += file
    }
    // println(s"[DataSorter] Flushed ${sorted.size} records to ${file.getName}")
  }
  
  private def compare(a: Array[Byte], b: Array[Byte]): Int = {
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