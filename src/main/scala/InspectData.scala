import java.io._
import common.Record
import org.slf4j.LoggerFactory

object InspectData {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    val files = Seq(
      new File("data/output1/partition.1"),
      new File("data/output2/partition.2"),
      new File("data/output3/partition.3")
    )

    files.foreach { file =>
      if (file.exists()) {
        logger.info("==================================================")
        logger.info(s"File: ${file.getPath}")
        logger.info(s"Size: ${file.length()} bytes")
        logger.info("==================================================")
        printContent(file)
      } else {
        logger.warn(s"File not found: ${file.getPath}")
      }
    }
  }

  def printContent(file: File): Unit = {
    val bis = new BufferedInputStream(new FileInputStream(file))
    val buffer = new Array[Byte](Record.SIZE)
    var count = 0
    
    // 앞부분 5개 저장용
    val headRecords = new scala.collection.mutable.ListBuffer[String]()
    // 마지막 레코드 저장용
    var lastRecordHex: String = ""

    try {
      while (bis.read(buffer) == Record.SIZE) {
        count += 1
        val keyBytes = buffer.slice(0, 10) // Key 10바이트만 추출
        val keyHex = bytesToHex(keyBytes)
        
        if (count <= 5) {
          headRecords += keyHex
        }
        lastRecordHex = keyHex
      }
    } finally {
      bis.close()
    }

    logger.info(s"Total Records: $count")
    logger.info("First 5 Keys (Hex):")
    headRecords.zipWithIndex.foreach { case (hex, idx) =>
      logger.info(s"   [$idx] $hex")
    }
    
    if (count > 5) {
      logger.info("   ...")
      logger.info("Last Key (Hex):")
      logger.info(s"   [${count-1}] $lastRecordHex")
    }
  }

  // 바이트 배열을 16진수 문자열로 변환 (보기 좋게)
  def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map("%02X".format(_)).mkString(" ")
  }
}