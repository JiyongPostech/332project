package worker

import scala.collection.mutable.ArrayBuffer

/** 데이터 정렬 모듈 */
class DataSorter {
  
  // 정렬된 데이터를 저장하는 버퍼
  private val sortedData = ArrayBuffer[Long]()
  
  /**
   * 이진 탐색을 이용하여 데이터를 삽입 정렬
   * @param value 삽입할 값
   */
  def insert(value: Long): Unit = {
    val index = binarySearchInsertPosition(value)
    sortedData.insert(index, value)
  }
  
  /**
   * 여러 값을 한꺼번에 삽입
   * @param values 삽입할 값들
   */
  def insertAll(values: Seq[Long]): Unit = {
    values.foreach(insert)
  }
  
  /**
   * 이진 탐색으로 삽입 위치 찾기
   * @param value 삽입할 값
   * @return 삽입할 인덱스
   */
  private def binarySearchInsertPosition(value: Long): Int = {
    var left = 0
    var right = sortedData.size
    
    while (left < right) {
      val mid = left + (right - left) / 2
      if (sortedData(mid) < value) {
        left = mid + 1
      } else {
        right = mid
      }
    }
    
    left
  }
  
  /**
   * 정렬된 데이터 가져오기
   * @return 정렬된 데이터
   */
  def getSortedData: Seq[Long] = sortedData.toSeq
  
  /**
   * 데이터 초기화
   */
  def clear(): Unit = sortedData.clear()
  
  /**
   * 현재 저장된 데이터 개수
   */
  def size: Int = sortedData.size
  
  /**
   * 일괄 정렬 (기존 데이터가 있을 때 사용)
   * @param data 정렬할 데이터
   * @return 정렬된 데이터
   */
  def sort(data: Seq[Long]): Seq[Long] = {
    data.sorted
  }
  
  /**
   * 정렬된 데이터를 파일에 저장
   * @param outputPath 출력 파일 경로
   */
  def saveToFile(outputPath: String): Unit = {
    val writer = new java.io.PrintWriter(outputPath)
    try {
      sortedData.foreach(writer.println)
      println(s"[DataSorter] Saved ${sortedData.size} items to $outputPath")
    } finally {
      writer.close()
    }
  }
}
