import java.io.File
import worker.FileIO
import common.Record

object ValidateSort extends App {
  
  // 파티션 1부터 10까지 검색하도록 설정
  val fileRange = 1 to 10
  
  fileRange.foreach { i =>
    val fileName = s"output/partition.$i" // [수정] 실제 저장 경로 사용
    val file = new File(fileName)
    
    if (file.exists()) {
      println(s"Checking $fileName...")
      
      val records = FileIO.readRecords(file).toSeq
      
      if (records.isEmpty) {
        println(" -> [PASS] Empty file.")
      } else {
        var prevKey: Array[Byte] = null
        var isSorted = true
        
        records.foreach { rec =>
          if (prevKey != null) {
            // KeyOrdering을 사용하여 비교
            if (Record.KeyOrdering.compare(prevKey, rec.key) > 0) {
              isSorted = false
            }
          }
          prevKey = rec.key
        }
        
        if (isSorted) {
          println(" -> [PASS] Sorted correctly.")
        } else {
          println(" -> [FAIL] Not sorted.")
        }
      }
    } else {
      // 화면 출력 제거 (InspectData에서만 확인하도록)
    }
  }
}
