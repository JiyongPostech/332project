import java.io._
import scala.util.Random
import org.slf4j.LoggerFactory

object MakeData {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    // 폴더 생성
    new File("data/input1").mkdirs()
    new File("data/input2").mkdirs()
    new File("data/input3").mkdirs()
    new File("data/output1").mkdirs()
    new File("data/output2").mkdirs()
    new File("data/output3").mkdirs()

    // 데이터 생성 (각 워커당 파일 2개씩, 파일당 1000개 레코드)
    createFile("data/input1/file1.dat", 1000)
    createFile("data/input1/file2.dat", 1000)
    
    createFile("data/input2/file1.dat", 1000)
    createFile("data/input2/file2.dat", 1000)
    
    createFile("data/input3/file1.dat", 1000)
    createFile("data/input3/file2.dat", 1000)
    
    logger.info("테스트 데이터 생성 완료!")
  }

  def createFile(path: String, count: Int): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(path))
    val rand = new Random()
    try {
      for (_ <- 1 to count) {
        val bytes = new Array[Byte](100)
        rand.nextBytes(bytes) // 100바이트 랜덤 데이터
        bos.write(bytes)
      }
    } finally {
      bos.close()
    }
    logger.info(s"Created $path")
  }
}