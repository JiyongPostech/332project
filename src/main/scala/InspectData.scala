import java.io._
import common.Record

object InspectData {
  def main(args: Array[String]): Unit = {
    val files = Seq(
      new File("data/output1/partition.1"),
      new File("data/output2/partition.2"),
      new File("data/output3/partition.3")
    )

    files.foreach { file =>
      if (file.exists()) {
        println(s"\n==================================================")
        println(s" 📂 File: ${file.getPath}")
        println(s" 📦 Size: ${file.length()} bytes")
        println(s"==================================================")
        printContent(file)
      } else {
        println(s"❌ File not found: ${file.getPath}")
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

    println(s"📊 Total Records: $count")
    println(s"⬇️  First 5 Keys (Hex):")
    headRecords.zipWithIndex.foreach { case (hex, idx) =>
      println(s"   [$idx] $hex")
    }
    
    if (count > 5) {
      println("   ...")
      println(s"⬇️  Last Key (Hex):")
      println(s"   [${count-1}] $lastRecordHex")
    }
  }

  // 바이트 배열을 16진수 문자열로 변환 (보기 좋게)
  def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map("%02X".format(_)).mkString(" ")
  }
}