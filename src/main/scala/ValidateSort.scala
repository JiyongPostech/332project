import java.io._
import java.util.Arrays
import common.Record
import org.slf4j.LoggerFactory

object ValidateSort {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    // 검증할 파일 목록
    val files = Seq(
      new File("data/output1/partition.1"),
      new File("data/output2/partition.2"),
      new File("data/output3/partition.3")
    )

    files.foreach { file =>
      if (file.exists()) {
        logger.info(s"Checking ${file.getName}...")
        if (checkSorted(file)) logger.info(s"  -> [PASS] Sorted correctly")
        else logger.error(s"  -> [FAIL] Not sorted!")
      } else {
        logger.warn(s"File not found: ${file.getName}")
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
          logger.error("Error: Unsorted records found")
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