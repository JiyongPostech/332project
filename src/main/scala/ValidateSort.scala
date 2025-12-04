import java.io._
import java.util.Arrays
import common.Record

object ValidateSort {
  def main(args: Array[String]): Unit = {
    // 검증할 파일 목록
    val files = Seq(
      new File("data/output1/partition.1"),
      new File("data/output2/partition.2"),
      new File("data/output3/partition.3")
    )

    files.foreach { file =>
      if (file.exists()) {
        println(s"Checking ${file.getName}...")
        if (checkSorted(file)) println(s"  -> [PASS] Sorted correctly.")
        else println(s"  -> [FAIL] Not sorted!")
      } else {
        println(s"File not found: ${file.getName}")
      }
    }
  }

  def checkSorted(file: File): Boolean = {
    val bis = new BufferedInputStream(new FileInputStream(file))
    val buffer = new Array[Byte](Record.SIZE)
    var prev: Record = null
    
    try {
      while (bis.read(buffer) == Record.SIZE) {
        val current = Record(buffer.clone())
        if (prev != null && prev.compare(current) > 0) {
          println(s"Error: Unsorted records found.")
          return false
        }
        prev = current
      }
    } finally {
      bis.close()
    }
    true
  }
}