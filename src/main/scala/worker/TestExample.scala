package worker

import scala.collection.mutable

/**
 * 테스트용 예시 코드
 * 네트워크 없이 WorkerRuntime의 receiveData와 sendDataToWorker 함수를 테스트합니다.
 * 
 * 사용 방법:
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
    
    // Worker 1이 Worker 2에게 정렬 데이터 전송 (dataType=0)
    worker1.sendDataToWorker(targetWorkerId = 2, dataType = 0, dataForWorker2)
    
    // Worker 2가 수신했다고 시뮬레이션
    println("[Worker 2] 데이터 수신 중...")
    worker2.receiveData(dataType = 0, dataForWorker2)
    
    val sortedInWorker2 = worker2.runSort()
    println(s"[Worker 2] 정렬 완료: $sortedInWorker2")
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
