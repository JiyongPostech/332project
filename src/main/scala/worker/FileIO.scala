package worker

import common.Record
import java.io._

object FileIO {
  def readRecords(file: File): Iterator[Record] = new Iterator[Record] {
    val bis = new BufferedInputStream(new FileInputStream(file))
    val buffer = new Array[Byte](Record.SIZE)
    var nextRecord: Option[Record] = fetchNext()

    private def fetchNext(): Option[Record] = {
      // EOF 처리 및 정확히 100바이트 읽기
      val read = bis.read(buffer)
      if (read == Record.SIZE) Some(Record(buffer.clone())) 
      else { bis.close(); None }
    }
    override def hasNext: Boolean = nextRecord.isDefined
    override def next(): Record = { val r = nextRecord.get; nextRecord = fetchNext(); r }
  }

  def writeRecords(file: File, records: Seq[Record]): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(file))
    try {
      records.foreach(r => bos.write(r.toBytes))
    } finally {
      bos.close()
    }
  }
}