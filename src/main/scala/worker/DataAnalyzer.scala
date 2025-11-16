package worker

import scala.collection.mutable.ArrayBuffer

/** 데이터 분석 결과 */
case class AnalysisResult(
  minValue: Long,
  maxValue: Long,
  count: Int,
  samples: Seq[Long]  // 샘플링된 데이터
)

/** 워커가 받은 데이터를 분석하는 모듈 */
class DataAnalyzer(sampleRate: Double = 0.01) {
  
  /**
   * 데이터를 분석하여 통계 정보와 샘플을 추출
   * @param data 분석할 데이터
   * @return 분석 결과
   */
  def analyze(data: Seq[Long]): AnalysisResult = {
    if (data.isEmpty) {
      return AnalysisResult(0, 0, 0, Seq.empty)
    }
    
    val minValue = data.min
    val maxValue = data.max
    val count = data.size
    
    // 샘플링: 전체 데이터의 일부만 추출
    val sampleSize = Math.max(1, (data.size * sampleRate).toInt)
    val step = Math.max(1, data.size / sampleSize)
    val samples = data.zipWithIndex
      .filter { case (_, idx) => idx % step == 0 }
      .map(_._1)
      .take(sampleSize)
    
    AnalysisResult(minValue, maxValue, count, samples)
  }
  
  /**
   * 파일에서 데이터를 읽어 분석
   * @param filePath 데이터 파일 경로
   * @return 분석 결과
   */
  def analyzeFromFile(filePath: String): AnalysisResult = {
    val data = readDataFromFile(filePath)
    analyze(data)
  }
  
  /**
   * 파일에서 데이터 읽기 (임시 구현)
   */
  private def readDataFromFile(filePath: String): Seq[Long] = {
    val source = scala.io.Source.fromFile(filePath)
    try {
      source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.toLong)
        .toSeq
    } finally {
      source.close()
    }
  }
}
