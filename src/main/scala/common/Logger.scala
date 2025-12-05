package common
import java.io.{FileWriter, PrintWriter, File}
import java.text.SimpleDateFormat
import java.util.Date

object Logger {
  private var writer: PrintWriter = _
  private val sdf = new SimpleDateFormat("HH:mm:ss.SSS")

  // 로그 파일 열기 (덮어쓰기 모드)
  def init(fileName: String): Unit = {
    writer = new PrintWriter(new FileWriter(new File(fileName), false), true)
  }

  // 파일에만 기록 (화면 출력 X)
  def info(msg: String): Unit = {
    if (writer != null) {
      writer.println(s"[${sdf.format(new Date())}] $msg")
    }
  }
  
  // 화면과 파일 모두 기록 (중요한 정보용)
  def printAndLog(msg: String): Unit = {
    println(msg)
    info(msg)
  }

  def close(): Unit = {
    if (writer != null) writer.close()
  }
}
