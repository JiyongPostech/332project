package worker

import common.Record
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

class DataSorter(tempDir: File) {
  if (!tempDir.exists()) tempDir.mkdirs()

  private val MAX_RECORDS = 50000 // 메모리 버퍼 크기 조절
  private var activeBuffer = new ArrayBuffer[Record](MAX_RECORDS)
  private val flushQueue = new LinkedBlockingQueue[ArrayBuffer[Record]]()
  val tempFiles = new ArrayBuffer[File]()

  // [수정] 변수명을 'thread' -> 'flushThread'로 변경하여 통일
  private val flushThread = new Thread(() => {
    while (!Thread.currentThread().isInterrupted) {
      try {
        val buffer = flushQueue.take()
        if (buffer.isEmpty) {
          Thread.currentThread().interrupt() // 종료 신호
        } else {
          // Record가 Ordered를 구현하므로 .sorted 사용 가능
          val sorted = buffer.sorted 
          
          val file = new File(tempDir, s"run_${System.nanoTime()}.dat")
          FileIO.writeRecords(file, sorted.toSeq)
          synchronized { tempFiles += file }
        }
      } catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    }
  })

  def start(): Unit = flushThread.start()

  def addRecord(record: Record): Unit = synchronized {
    activeBuffer += record
    if (activeBuffer.size >= MAX_RECORDS) {
      flushQueue.put(activeBuffer)
      activeBuffer = new ArrayBuffer[Record](MAX_RECORDS)
    }
  }

  def close(): Unit = {
    synchronized { if (activeBuffer.nonEmpty) flushQueue.put(activeBuffer) }
    flushQueue.put(new ArrayBuffer[Record]()) // Poison Pill
    try { flushThread.join() } catch { case _: InterruptedException => }
  }
}