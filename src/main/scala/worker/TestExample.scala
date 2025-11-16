// src/main/scala/worker/TestExample.scala
package worker

import scala.collection.mutable

/**
 * 테스트용 예시 코드
 * 네트워크 없이 WorkerRuntime의 receiveData와 sendDataToWorker 함수를 테스트합니다.
 * * 사용 방법:
 * 1. 이 파일을 실행하여 독립적으로 테스트
 * 2. 리시버 구현 시 참고용 예시로 활용
 */


object TestExample {
  
  def main(args: Array[String]): Unit = {
    println("=== WorkerRuntime Test Example ===\n")
    
    // 테스트 시나리오 1: 분석 데이터 수신 및 처리
    testAnalysisDataFlow()
    
    println("\n" + "="*50 + "\n")
    
    // 테스트 시나리오 2: 정렬 데이터 수신 및 처리
    testSortingDataFlow()
    
    println("\n" + "="*50 + "\n")
    
    // 테스트 시나리오 3: 워커 간 데이터 전송 시뮬레이션
    testWorkerCommunication()
    
    println("\n" + "="*80 + "\n")
    
    // 테스트 시나리오 4: 실제 분산 정렬 전체 파이프라인
    testFullDistributedSortingPipeline()
  }
  
  /**
   * 시나리오 1: 분석 데이터 흐름 테스트
   * 리시버가 dataType=1로 데이터를 받아서 분석 함수를 호출
   */
  def testAnalysisDataFlow(): Unit = {
    println("### Test 1: 분석 데이터 수신 및 처리 ###")
    
    // Mock NetworkService (실제 네트워크 없이 테스트)
    val mockNet = new MockNetworkService()
    val runtime = new WorkerRuntime(1, mockNet)
    
    // 리시버가 데이터를 받았다고 가정 (dataType=1: 분석 데이터)
    val receivedData = Seq(100L, 50L, 200L, 150L, 75L)
    println(s"리시버가 ${receivedData.size}개 데이터 수신: $receivedData")
    
    // receiveData 호출 (리시버가 할 작업)
    runtime.receiveData(dataType = 1, receivedData)
    
    // 분석 실행
    val analysisResult = runtime.runAnalysisFromReceivedData()
    println(s"분석 결과: min=${analysisResult.minValue}, max=${analysisResult.maxValue}, count=${analysisResult.count}")
    println(s"샘플: ${analysisResult.samples}")
  }
  
  /**
   * 시나리오 2: 정렬 데이터 흐름 테스트
   * 리시버가 dataType=0으로 데이터를 받아서 정렬 함수를 호출
   */
  def testSortingDataFlow(): Unit = {
    println("### Test 2: 정렬 데이터 수신 및 처리 ###")
    
    val mockNet = new MockNetworkService()
    val runtime = new WorkerRuntime(2, mockNet)
    
    // 리시버가 여러 번에 걸쳐 데이터를 받는다고 가정
    val batch1 = Seq(50L, 30L, 70L)
    val batch2 = Seq(10L, 90L, 20L)
    val batch3 = Seq(60L, 40L, 80L)
    
    println(s"\n[입력] 배치 1 수신: $batch1")
    runtime.receiveData(dataType = 0, batch1)
    println(s"        현재까지 정렬된 상태: ${runtime.getSortedData}")
    
    println(s"\n[입력] 배치 2 수신: $batch2")
    runtime.receiveData(dataType = 0, batch2)
    println(s"        현재까지 정렬된 상태: ${runtime.getSortedData}")
    
    println(s"\n[입력] 배치 3 수신: $batch3")
    runtime.receiveData(dataType = 0, batch3)
    println(s"        현재까지 정렬된 상태: ${runtime.getSortedData}")
    
    // 정렬 결과 확인
    println(s"\n[결과] 최종 정렬 확인:")
    val sortedData = runtime.runSort()
    println(s"       입력된 모든 데이터: [50, 30, 70, 10, 90, 20, 60, 40, 80]")
    println(s"       정렬된 결과 (${sortedData.size}개): $sortedData")
  }
  
  /**
   * 시나리오 3: 워커 간 통신 시뮬레이션
   * Worker 1이 데이터를 처리하고 Worker 2에게 전송
   */
  def testWorkerCommunication(): Unit = {
    println("### Test 3: 워커 간 데이터 전송 시뮬레이션 ###")
    
    // 두 워커 생성
    val mockNet1 = new MockNetworkService()
    val mockNet2 = new MockNetworkService()
    val worker1 = new WorkerRuntime(1, mockNet1)
    val worker2 = new WorkerRuntime(2, mockNet2)
    
    // Worker 1이 데이터를 처리
    val dataForWorker2 = Seq(100L, 200L, 300L)
    println(s"[Worker 1] Worker 2에게 전송할 데이터: $dataForWorker2")
    
    // [수정 3] Worker 1이 Worker 2에게 정렬 데이터 전송
    // 'sendDataToWorker' 대신 MockNetworkService의 'send'를 직접 호출
    val payload = dataForWorker2.mkString(",")
    mockNet1.send(targetId = 2, message = payload)
    
    // Worker 2가 수신했다고 시뮬레이션
    println("[Worker 2] 데이터 수신 중...")
    // (수정) onPeerMessage가 payload를 받으므로 dataType=0으로 receiveData 호출
    worker2.receiveData(dataType = 0, dataForWorker2)
    
    val sortedInWorker2 = worker2.runSort()
    println(s"[Worker 2] 정렬 완료: $sortedInWorker2")
  }
  
  /**
   * 시나리오 4: 실제 분산 정렬 전체 파이프라인 시뮬레이션
   * 마스터 데이터 분배 → 각 워커 분석 → 키 범위 계산 → Shuffle → 정렬 → 병합
   */
  def testFullDistributedSortingPipeline(): Unit = {
    println("### Test 4: 전체 분산 정렬 파이프라인 시뮬레이션 ###\n")
    
    // ========== 0단계: 마스터 데이터 준비 ==========
    println("━━━━━ 0단계: 마스터가 초기 데이터 준비 ━━━━━")
    val masterData = Seq(
      95L, 23L, 67L, 41L, 88L, 12L, 76L, 34L, 59L, 82L,
      15L, 48L, 91L, 27L, 63L, 39L, 74L, 56L, 85L, 19L
    )
    println(s"[마스터] 전체 데이터 (${masterData.size}개): ${masterData.sorted}")
    println(s"[마스터] 3개 워커에게 균등 분배 시작...\n")
    
    // ========== 1단계: 마스터가 워커들에게 데이터 분배 ==========
    println("━━━━━ 1단계: 데이터 분배 ━━━━━")
    val worker1 = new WorkerRuntime(1, new MockNetworkService())
    val worker2 = new WorkerRuntime(2, new MockNetworkService())
    val worker3 = new WorkerRuntime(3, new MockNetworkService())
    
    val chunk1 = masterData.slice(0, 7)
    val chunk2 = masterData.slice(7, 14)
    val chunk3 = masterData.slice(14, 20)
    
    println(s"[마스터 → Worker 1] ${chunk1.size}개: $chunk1")
    worker1.receiveData(1, chunk1)
    
    println(s"[마스터 → Worker 2] ${chunk2.size}개: $chunk2")
    worker2.receiveData(1, chunk2)
    
    println(s"[마스터 → Worker 3] ${chunk3.size}개: $chunk3")
    worker3.receiveData(1, chunk3)
    
    // ========== 2단계: 각 워커가 로컬 분석 수행 ==========
    println("\n━━━━━ 2단계: 각 워커 데이터 분석 ━━━━━")
    val analysis1 = worker1.runAnalysisFromReceivedData()
    println(s"[Worker 1] 분석: min=${analysis1.minValue}, max=${analysis1.maxValue}, count=${analysis1.count}")
    
    val analysis2 = worker2.runAnalysisFromReceivedData()
    println(s"[Worker 2] 분석: min=${analysis2.minValue}, max=${analysis2.maxValue}, count=${analysis2.count}")
    
    val analysis3 = worker3.runAnalysisFromReceivedData()
    println(s"[Worker 3] 분석: min=${analysis3.minValue}, max=${analysis3.maxValue}, count=${analysis3.count}")
    
    // ========== 3단계: 마스터가 키 범위 계산 및 브로드캐스트 ==========
    println("\n━━━━━ 3단계: 마스터가 키 범위 계산 ━━━━━")
    val globalMin = Seq(analysis1, analysis2, analysis3).map(_.minValue).min
    val globalMax = Seq(analysis1, analysis2, analysis3).map(_.maxValue).max
    
    println(s"[마스터] 전역 범위: min=$globalMin, max=$globalMax")
    
    // 균등 분할 (실제는 샘플 기반 분위수)
    val range = globalMax - globalMin + 1
    val rangePerWorker = (range + 2) / 3
    
    val keyRanges = Seq(
      common.KeyRange(1, globalMin, globalMin + rangePerWorker - 1),
      common.KeyRange(2, globalMin + rangePerWorker, globalMin + 2 * rangePerWorker - 1),
      common.KeyRange(3, globalMin + 2 * rangePerWorker, globalMax)
    )
    
    println(s"[마스터] 키 범위 배정:")
    keyRanges.foreach { kr =>
      println(s"  Worker ${kr.workerId}: [${kr.minKey} ~ ${kr.maxKey}]")
    }
    
    // 워커들에게 키 범위 전달 (실제는 TCP 메시지)
    keyRanges.foreach { kr =>
      if (kr.workerId == 1) simulateKeyRangeUpdate(worker1, keyRanges)
      else if (kr.workerId == 2) simulateKeyRangeUpdate(worker2, keyRanges)
      else if (kr.workerId == 3) simulateKeyRangeUpdate(worker3, keyRanges)
    }
    
    // ========== 4단계: 데이터 재분배 (Shuffle) ==========
    println("\n━━━━━ 4단계: 데이터 Shuffle (재분배) ━━━━━")
    
    val data1 = worker1.getReceivedData
    val data2 = worker2.getReceivedData
    val data3 = worker3.getReceivedData
    
    println(s"[Worker 1] Shuffle 전 데이터: $data1")
    println(s"[Worker 2] Shuffle 전 데이터: $data2")
    println(s"[Worker 3] Shuffle 전 데이터: $data3")
    
    // 각 워커가 데이터를 키 범위에 따라 재분배
    val shuffleResult = performManualShuffle(
      Seq((1, data1), (2, data2), (3, data3)),
      keyRanges
    )
    
    println(s"\n[Shuffle 결과]")
    shuffleResult.foreach { case (wid, data) =>
      println(s"  Worker ${wid}로 전달된 데이터 (${data.size}개): ${data.sorted}")
    }
    
    // 워커들에게 shuffle된 데이터 전달
    shuffleResult.foreach { case (wid, data) =>
      val worker = if (wid == 1) worker1 else if (wid == 2) worker2 else worker3
      worker.receiveData(0, data)
    }
    
    // ========== 5단계: 각 워커가 로컬 정렬 ==========
    println("\n━━━━━ 5단계: 각 워커 로컬 정렬 ━━━━━")
    val sorted1 = worker1.runSort()
    println(s"[Worker 1] 정렬 완료 (${sorted1.size}개): $sorted1")
    
    val sorted2 = worker2.runSort()
    println(s"[Worker 2] 정렬 완료 (${sorted2.size}개): $sorted2")
    
    val sorted3 = worker3.runSort()
    println(s"[Worker 3] 정렬 완료 (${sorted3.size}개): $sorted3")
    
    // ========== 6단계: 마스터가 최종 병합 ==========
    println("\n━━━━━ 6단계: 마스터가 최종 병합 ━━━━━")
    val finalResult = sorted1 ++ sorted2 ++ sorted3
    
    println(s"[마스터] 최종 병합 결과 (${finalResult.size}개):")
    println(s"        $finalResult")
    
    // 검증
    println(s"\n[검증]")
    println(s"  원본 정렬: ${masterData.sorted}")
    println(s"  결과 정렬: $finalResult")
    println(s"  정렬 성공: ${masterData.sorted == finalResult}")
  }
  
  /**
   * 키 범위 업데이트 시뮬레이션 (실제는 네트워크 메시지)
   */
  private def simulateKeyRangeUpdate(worker: WorkerRuntime, ranges: Seq[common.KeyRange]): Unit = {
    // WorkerRuntime의 private 메서드를 호출할 수 없으므로 직접 처리
    // 실제로는 onKeyRangeFromMaster 콜백이 호출됨
    // 이 테스트는 onKeyRangeFromMaster를 직접 테스트하지 않으므로 비워둠
  }
  
  /**
   * 수동으로 Shuffle 수행 (실제는 워커들이 P2P로 전송)
   */
  private def performManualShuffle(
    workerData: Seq[(Int, Seq[Long])],
    keyRanges: Seq[common.KeyRange]
  ): Map[Int, Seq[Long]] = {
    import scala.collection.mutable
    
    val result = mutable.Map[Int, mutable.ArrayBuffer[Long]](
      1 -> mutable.ArrayBuffer.empty,
      2 -> mutable.ArrayBuffer.empty,
      3 -> mutable.ArrayBuffer.empty
    )
    
    workerData.foreach { case (fromWorker, data) =>
      data.foreach { value =>
        // 이 값이 어느 워커의 범위에 속하는지 찾기
        val targetWorker = keyRanges.find { kr =>
          value >= kr.minKey && value <= kr.maxKey
        }.map(_.workerId).getOrElse(1)
        
        result(targetWorker) += value
      }
    }
    
    result.view.mapValues(_.toSeq).toMap
  }
}

/**
 * 테스트용 Mock NetworkService
 * 실제 네트워크 없이 함수 호출만 확인
 */
class MockNetworkService extends network.NetworkService {
  private val sentMessages = mutable.ArrayBuffer[(Int, String)]()
  private val masterMessages = mutable.ArrayBuffer[String]()
  
  override def bind(onMessageReceived: (Int, String) => Unit): Unit = {
    println("[MockNetwork] bind() 호출됨")
  }
  
  override def connect_to_master(
    onPeerListReceived: (Map[Int, java.net.InetSocketAddress]) => Unit,
    onPeerJoined: (Int, java.net.InetSocketAddress) => Unit,
    onPeerLeft: (Int) => Unit,
    onKeyRange: Seq[(Int, Long, Long)] => Unit,
    onInitialData: String => Unit
  ): Unit = {
    println("[MockNetwork] connect_to_master() 호출됨")
  }
  
  override def send(targetId: Int, message: String): Unit = {
    sentMessages += ((targetId, message))
    println(s"[MockNetwork] 전송: targetId=$targetId, message=${message.take(50)}...")
  }
  
  override def send_to_master(message: String): Unit = {
    masterMessages += message
    println(s"[MockNetwork] 마스터에게 전송: ${message.take(50)}...")
  }
  
  override def stop(): Unit = {
    println("[MockNetwork] stop() 호출됨")
  }
  
  def getSentMessages: Seq[(Int, String)] = sentMessages.toSeq
  def getMasterMessages: Seq[String] = masterMessages.toSeq
}