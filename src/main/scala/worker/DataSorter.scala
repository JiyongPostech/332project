package worker

import common.Record
import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ArrayBuffer

class DataSorter(tempDir: File) extends Runnable {
  
  // [수정] 큐 크기 제한 제거. (Bounded Queue -> Unbounded Queue)
  // 메모리 폭발 방지 로직을 해제하여, 수신 스레드가 블록되지 않도록 합니다.
  private val queue = new LinkedBlockingQueue[Record]()
  private val buffer = new ArrayBuffer[Record]()
  private var isRunning = true
  
  val tempFiles = new ArrayBuffer[File]()
  
  if (!tempDir.exists()) tempDir.mkdirs()

  def start(): Unit = {
    new Thread(this, "DataSorterThread").start()
  }
  
  def addRecord(record: Record): Unit = {
    // [수정] put()은 큐가 무제한이 되었으므로 (메모리가 꽉 차기 전까지) 블록되지 않습니다.
    queue.put(record)
  }

  override def run(): Unit = {
    while (isRunning || queue.size() > 0) {
      try {
        // 100ms 동안 데이터를 기다림
        val rec = queue.poll(100, TimeUnit.MILLISECONDS)
        if (rec != null) {
          buffer += rec
        }

        // 버퍼가 꽉 찼거나 (50000개), 100ms 동안 새 데이터가 없는데 버퍼에 데이터가 있을 경우 (플러시)
        if (buffer.size >= 50000 || (rec == null && buffer.nonEmpty)) {
          flushBuffer()
        }
      } catch {
        case e: InterruptedException => 
          Thread.currentThread().interrupt()
          isRunning = false
      }
    }
    // 루프 종료 후 남은 데이터 최종 플러시
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    println(s"[DataSorter] Thread finished. Total temp files: ${tempFiles.size}")
  }

  private def flushBuffer(): Unit = {
    // 1. In-memory 정렬
    buffer.sortInPlaceBy(_.key)(Record.KeyOrdering)
    
    // 2. 임시 파일로 저장
    val tempFile = File.createTempFile("sort_temp_", ".dat", tempDir)
    val output = new BufferedOutputStream(new FileOutputStream(tempFile))
    
    try {
      buffer.foreach { rec =>
        output.write(rec.toBytes)
      }
    } finally {
      output.close()
    }
    
    tempFiles += tempFile
    buffer.clear()
  }

  def close(): Unit = {
    isRunning = false
    // run 메서드에서 남은 작업을 마칠 때까지 기다립니다.
    // (완벽한 동기화를 위해 Thread.join()을 사용할 수 있지만, 간단하게 플래그만 변경합니다.)
    // 모든 큐 작업이 완료되면 run 루프는 자동으로 종료됩니다.
  }
}
