package worker

import common.Record
import java.io._
import java.util.PriorityQueue

class DiskMerger {
  case class Entry(record: Record, iter: Iterator[Record]) extends Comparable[Entry] {
    override def compareTo(o: Entry): Int = this.record.compare(o.record)
  }

  def merge(inputs: Seq[File], outputFile: File): Unit = {
    val pq = new PriorityQueue[Entry]()
    
    // 각 파일의 첫 레코드를 큐에 삽입
    inputs.foreach { f =>
      val it = FileIO.readRecords(f)
      if (it.hasNext) pq.add(Entry(it.next(), it))
    }

    val bos = new BufferedOutputStream(new FileOutputStream(outputFile))
    try {
      while (!pq.isEmpty) {
        val entry = pq.poll()
        bos.write(entry.record.toBytes)
        
        if (entry.iter.hasNext) {
          pq.add(Entry(entry.iter.next(), entry.iter))
        }
      }
    } finally {
      bos.close()
    }
    println(s"[Merger] Result saved to ${outputFile.getAbsolutePath}")
  }
}