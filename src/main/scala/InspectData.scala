import java.io.File
import worker.FileIO
import common.Record

object InspectData extends App {
  
  val fileRange = 1 to 10
  var totalOutputRecords = 0L // [추가] 총 레코드 수 카운터

  println("--- Partition Inspection ---")
  
  fileRange.foreach { i =>
    val fileName = s"output/partition.$i" 
    val file = new File(fileName)
    
    if (file.exists()) {
      println(s"========================================")
      println(s" Inspecting: $fileName")
      
      // FileIO.readRecords는 Iterator를 반환하므로 .toSeq로 메모리에 로드
      val records = FileIO.readRecords(file).toSeq 
      
      if (records.nonEmpty) {
        println(s" Record Count: ${records.size}")
        totalOutputRecords += records.size // [핵심] 총합에 더함
        
        println(" First 5 Keys (Hex):")
        records.take(5).zipWithIndex.foreach { case (rec, index) =>
          println(s"  [${index}] ${rec.key.map("%02X".format(_)).mkString}")
        }
        println(s" Last Key (Hex): ${records.last.key.map("%02X".format(_)).mkString}")
      } else {
        println(" Empty file.")
      }
    } else {
      // 파일이 없을 경우 출력하지 않음
    }
  }

  // [추가] 최종적으로 총 레코드 수 출력
  println("\n========================================")
  println(s"✅ Total Output Records: $totalOutputRecords")
  println("========================================")
}
