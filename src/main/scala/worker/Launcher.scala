package worker

import common._
import network._

object Launcher {
  def main(args: Array[String]): Unit = {
    println("=== Worker Launcher Started ===")
    
    // 1) 워커 ID 및 통신 초기화
    val workerId = if (args.nonEmpty) args(0).toInt else 1
    val myUdpPort = 50051 + workerId  // 워커별 포트 할당
    val masterHost = "localhost"
    val masterPort = 50051
    
    val net: NetworkService = new NettyClientService(
      workerId, 
      myUdpPort, 
      masterHost, 
      masterPort
    )

    // 2) 런타임 구성 및 시작
    val runtime = new WorkerRuntime(workerId, net)
    runtime.start()
    
    println(s"[Worker $workerId] Started and connected to master")

    // ===== 파이프라인 실행 (Main 깔끔하게 구조화) =====
    
    // 데모 시나리오: 마스터로부터 데이터를 받아 전체 파이프라인 실행
    val outputFile = s"data/output_worker_$workerId.txt"  // 각 워커의 출력 파일
    
    try {
      // === 0단계: 마스터로부터 초기 데이터 수신 대기 ===
      println(s"\n[Worker $workerId] === Step 0: Waiting for Initial Data from Master ===")
      if (!runtime.waitForInitialData(timeoutSeconds = 30)) {
        println(s"[Worker $workerId] Timeout waiting for initial data from master")
        println(s"[Worker $workerId] Falling back to local file read for demo")
        // 폴백: 로컬 파일 읽기 (데모용)
        val inputFile = s"data/input_worker_$workerId.txt"
        val localData = readDataFromFile(inputFile)
        // TODO: runtime에 직접 주입하는 메서드 필요
      }
      
      // === 1단계: 데이터 분석 ===
      println(s"\n[Worker $workerId] === Step 1: Data Analysis ===")
      val analysisResult = runtime.runAnalysisFromReceivedData()
      
      // 마스터에게 분석 결과 전송
      runtime.sendAnalysisToMaster(analysisResult)
      
      // === 2단계: 키 범위 수신 대기 ===
      println(s"\n[Worker $workerId] === Step 2: Waiting for Key Ranges from Master ===")
      if (!runtime.waitForKeyRange(timeoutSeconds = 30)) {
        println(s"[Worker $workerId] Timeout waiting for key ranges from master")
        return
      }
      
      // === 3단계: 데이터 재분배 (Shuffle) ===
      println(s"\n[Worker $workerId] === Step 3: Data Shuffle ===")
      // 수신한 데이터로 Shuffle
      val dataToShuffle = runtime.getReceivedData
      val myData = runtime.runShuffle(dataToShuffle, runtime.getKeyRanges)
      
      // === 4단계: 데이터 정렬 ===
      println(s"\n[Worker $workerId] === Step 4: Data Sorting ===")
      // Shuffle 데이터 수신 대기
      if (!runtime.waitForShuffleData(timeoutSeconds = 30)) {
        println(s"[Worker $workerId] Warning: Not all shuffle data received, proceeding with available data")
      }
      val sortedData = runtime.runSort()
      
      // === 5단계: 결과 저장 ===
      println(s"\n[Worker $workerId] === Step 5: Save Results ===")
      runtime.saveSortedData(outputFile)
      
      // === 6단계: 마스터에게 정렬된 데이터 전송 (최종 병합용) ===
      println(s"\n[Worker $workerId] === Step 6: Send Sorted Data to Master ===")
      runtime.sendSortedDataToMaster()
      
      println(s"\n[Worker $workerId] === Pipeline Complete ===")
      
    } catch {
      case e: Exception =>
        println(s"[Worker $workerId] Error during pipeline execution: ${e.getMessage}")
        e.printStackTrace()
    }

    // 종료 대기(데모용)
    println(s"\n[Worker $workerId] Press Enter to stop...")
    scala.io.StdIn.readLine()
    runtime.stop()
    println(s"[Worker $workerId] Stopped")
  }
  
  /**
   * 파일에서 데이터 읽기 (헬퍼 함수)
   */
  private def readDataFromFile(filePath: String): Seq[Long] = {
    try {
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
    } catch {
      case e: Exception =>
        println(s"Warning: Could not read file $filePath, using empty data")
        Seq.empty
    }
  }
}
