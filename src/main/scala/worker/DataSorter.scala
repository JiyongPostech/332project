package worker

import common.{Record, Logger}
import java.io._
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

class DataSorter(tempDir: File) {
  
  private val MAX_RECORDS = 50000 
  private val flushQueue = new LinkedBlockingQueue[ArrayBuffer[Record]]()
  private var activeBuffer = new ArrayBuffer[Record](MAX_RECORDS)
  
  val tempFiles = new ArrayBuffer[File]()
  
  if (!tempDir.exists()) tempDir.mkdirs()

  private val flushThread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        while (!Thread.currentThread().isInterrupted) {
          val buffer = flushQueue.take() 
          if (buffer.isEmpty) return
          
          // [수정 완료] Record.KeyOrdering을 사용하여 정렬
          val sorted = buffer.sortWith((a, b) => Record.KeyOrdering.compare(a.key, b.key) < 0)
          
          val file = new File(tempDir, s"run_${System.nanoTime()}.dat")
          FileIO.writeRecords(file, sorted.toSeq)
          
          synchronized { 
            tempFiles += file 
            Logger.info(s"[DataSorter] Created segment ${file.getName} with ${buffer.size} records.")
          }
        }
      } catch { 
        case _: InterruptedException => Thread.currentThread().interrupt() 
      }
    }
  }, "DataSorter-FlushThread")

  def start(): Unit = flushThread.start()

  def addRecord(record: Record): Unit = synchronized {
    activeBuffer += record
    if (activeBuffer.size >= MAX_RECORDS) {
      flushQueue.put(activeBuffer)
      activeBuffer = new ArrayBuffer[Record](MAX_RECORDS)
    }
  }

  def close(): Unit = {
    synchronized { 
      if (activeBuffer.nonEmpty) {
        Logger.info(s"[DataSorter] Flushing final active buffer (${activeBuffer.size} records).")
        flushQueue.put(activeBuffer)
      }
    }
    flushQueue.put(new ArrayBuffer[Record]()) // Poison Pill
    try { 
      flushThread.join() 
    } catch { case _: InterruptedException => }
    Logger.info(s"DataSorter finished. Total runs created: ${tempFiles.size}")
  }
}
