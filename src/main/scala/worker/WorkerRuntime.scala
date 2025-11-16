// src/main/scala/worker/WorkerRuntime.scala
package worker

import common._
import network._
import scala.collection.mutable

/** 워커 프로세스의 생명주기/이벤트 루프 (수정) */
class WorkerRuntime(
  id: Int,
  net: NetworkService
) {
  
  // 데이터 처리 모듈
  private val analyzer = new DataAnalyzer()
  // (수정) DataShuffler에 sendData 함수 (순수 payload 전송) 전달
  private val shuffler = new DataShuffler(id, sendData)
  private val sorter = new DataSorter()
  
  // 수신된 데이터 임시 저장
  private val receivedData = mutable.ArrayBuffer[Long]()
  private var keyRanges: Seq[KeyRange] = Seq.empty
  
  // 동기화를 위한 플래그들 (재사용 가능하도록 volatile boolean 사용)
  @volatile private var initialDataReceived = false
  @volatile private var keyRangeReceived = false
  private val keyRangeLock = new Object()
  private val initialDataLock = new Object()
  
  // Shuffle 데이터 수신 추적
  @volatile private var expectedShuffleMessages = 0
  @volatile private var receivedShuffleMessages = 0

  /** 시작: 마스터에 등록, 제어 메시지 수신 바인딩 */
  def start(): Unit = {
    // 1. P2P (UDP) 메시지 수신 콜백 등록
    // onPeerMessage는 이제 Netty의 "Receive 스레드"가 호출 (I/O 스레드 X)
    net.bind(onPeerMessage)
    
    // 2. 마스터 (TCP) 연결 (기존 콜백 유지)
    net.connect_to_master(
      onInitialPeers, 
      onPeerJoined, 
      onPeerLeft, 
      onKeyRangeFromMaster, 
      onInitialDataFromMaster
    )
  }

  /** 안전 종료 */
  def stop(): Unit = net.stop()

  /**
   * P2P 메시지 수신 콜백 (수정)
   * - 이 함수는 Netty의 "Receive 스레드"에 의해 호출됨 (I/O 스레드 아님)
   * - msg 파라미터는 "DATA:"가 없는 순수 payload (예: "100,200,300")
   */
  private def onPeerMessage(senderId: Int, msg: String): Unit = {
    try {
      // (수정) "DATA:" 접두사 체크 없이, 순수 payload만 처리
      val data = parseData(msg) // 내부 parseData 사용
      sorter.insertAll(data)
      receivedShuffleMessages += 1
      println(s"[Worker $id] Received ${data.size} items for sorting from $senderId ($receivedShuffleMessages/$expectedShuffleMessages)")
      
      // 모든 shuffle 데이터 수신 완료 확인
      if (expectedShuffleMessages > 0 && receivedShuffleMessages >= expectedShuffleMessages) {
        println(s"[Worker $id] All shuffle data received")
      }
    } catch {
      case e: Exception => println(s"onPeerMessage 처리 오류: $e, msg: $msg")
    }
  }
  
  /**
   * 데이터 파싱 (쉼표로 구분된 숫자)
   * (수정) DataShuffler.parseReceivedData 대신 이 함수를 직접 사용
   */
  private def parseData(dataStr: String): Seq[Long] = {
    if (dataStr.isEmpty) Seq.empty
    else dataStr.split(",").map(_.toLong).toSeq
  }
  
  /**
   * 키 범위 정보 파싱
   * 형식: "workerId:minKey:maxKey,workerId:minKey:maxKey,..."
   */
  private def parseKeyRanges(rangeStr: String): Seq[KeyRange] = {
    rangeStr.split(",").map { part =>
      val Array(wid, min, max) = part.split(":")
      KeyRange(wid.toInt, min.toLong, max.toLong)
    }.toSeq
  }
  
  /**
   * 다른 워커에게 데이터 전송 (내부용)
   * @param message 순수 Payload (예: "100,200,300")
   */
  private def sendData(targetWorkerId: Int, message: String): Unit = {
    net.send(targetWorkerId, message)
  }
  
  /**
   * 마스터에게 분석 결과 전송
   */
  def sendAnalysisToMaster(result: AnalysisResult): Unit = {
    val message = s"ANALYSIS:${result.minValue}:${result.maxValue}:${result.count}:${result.samples.mkString(",")}"
    net.send_to_master(message)
    println(s"[Worker $id] Sent analysis result to Master (TCP)")
  }
  
  private def onInitialPeers(peers: Map[Int, java.net.InetSocketAddress]): Unit = {
    println(s"[Worker $id] Connected to ${peers.size} peers")
  }
  
  private def onPeerJoined(peerId: Int, addr: java.net.InetSocketAddress): Unit = {
    println(s"[Worker $id] Peer $peerId joined")
  }
  
  private def onPeerLeft(peerId: Int): Unit = {
    println(s"[Worker $id] Peer $peerId left")
  }

  /** 마스터로부터 KEYRANGE 수신 (TCP 경로) */
  private def onKeyRangeFromMaster(ranges: Seq[(Int, Long, Long)]): Unit = {
    keyRanges = ranges.map { case (wid, minK, maxK) => KeyRange(wid, minK, maxK) }
    keyRangeLock.synchronized {
      keyRangeReceived = true
      keyRangeLock.notifyAll()
    }
    println(s"[Worker $id] KEYRANGE from master: ${keyRanges.mkString(", ")}")
  }
  
  /** 마스터로부터 INITIAL_DATA 수신 (TCP 경로) */
  private def onInitialDataFromMaster(message: String): Unit = {
    if (message.startsWith("INITIAL_DATA:")) {
      val dataStr = message.stripPrefix("INITIAL_DATA:").trim
      val data = parseData(dataStr)
      receivedData ++= data
      initialDataLock.synchronized {
        initialDataReceived = true
        initialDataLock.notifyAll()
      }
      println(s"[Worker $id] Received ${data.size} initial data items from Master (TCP)")
    }
  }
  
  /**
   * 키 범위를 기다림 (타임아웃 포함)
   * @param timeoutSeconds 대기 시간 (초)
   * @return 성공 여부
   */
  def waitForKeyRange(timeoutSeconds: Long = 10): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
    keyRangeLock.synchronized {
      while (!keyRangeReceived && System.currentTimeMillis() < deadline) {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining > 0) {
          try {
            keyRangeLock.wait(remaining)
          } catch {
            case _: InterruptedException => return false
          }
        }
      }
      keyRangeReceived
    }
  }
  
  /**
   * 초기 데이터를 기다림 (타임아웃 포함)
   * @param timeoutSeconds 대기 시간 (초)
   * @return 성공 여부
   */
  def waitForInitialData(timeoutSeconds: Long = 10): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
    initialDataLock.synchronized {
      while (!initialDataReceived && System.currentTimeMillis() < deadline) {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining > 0) {
          try {
            initialDataLock.wait(remaining)
          } catch {
            case _: InterruptedException => return false
          }
        }
      }
      initialDataReceived
    }
  }
  
  /**
   * 수신한 데이터 가져오기
   */
  def getReceivedData: Seq[Long] = receivedData.toSeq
  
  // === 외부에서 호출 가능한 파이프라인 함수들 ===
  
  /**
   * 외부 리시버가 데이터를 받아서 정렬/분석 함수에 넘길 때 호출
   * (이 함수는 현재 아키텍처에서 직접 사용되지는 않지만, TestExample을 위해 유지)
   * @param dataType 0: 정렬, 1: 분석
   * @param data 데이터 시퀀스
   */
  def receiveData(dataType: Int, data: Seq[Long]): Unit = {
    dataType match {
      case 0 => // 정렬 데이터
        sorter.insertAll(data)
        println(s"[Worker $id] Received ${data.size} items for sorting (receiver)")
      case 1 => // 분석 데이터
        receivedData ++= data
        println(s"[Worker $id] Received ${data.size} items for analysis (receiver)")
      case _ =>
        println(s"[Worker $id] Unknown dataType: $dataType")
    }
  }
  
  /**
   * 1단계: 데이터 분석
   */
  def runAnalysis(dataFilePath: String): AnalysisResult = {
    println(s"[Worker $id] Starting data analysis...")
    val result = analyzer.analyzeFromFile(dataFilePath)
    println(s"[Worker $id] Analysis complete: min=${result.minValue}, max=${result.maxValue}, count=${result.count}")
    result
  }
  
  /**
   * 1단계: 수신한 데이터로 분석 (마스터로부터 받은 데이터 사용)
   */
  def runAnalysisFromReceivedData(): AnalysisResult = {
    println(s"[Worker $id] Starting data analysis from received data...")
    val result = analyzer.analyze(receivedData.toSeq)
    println(s"[Worker $id] Analysis complete: min=${result.minValue}, max=${result.maxValue}, count=${result.count}")
    result
  }
  
  /**
   * 2단계: 데이터 재분배 (Shuffle)
   */
  def runShuffle(data: Seq[Long], ranges: Seq[KeyRange]): Seq[Long] = {
    println(s"[Worker $id] Starting data shuffle...")
    
    // Shuffle 전에 카운터 초기화
    receivedShuffleMessages = 0
    expectedShuffleMessages = ranges.size - 1  // 다른 모든 워커로부터 받을 예정
    
    val myData = shuffler.shuffle(data, ranges)
    println(s"[Worker $id] Shuffle complete: kept ${myData.size} items, expecting ${expectedShuffleMessages} messages from peers")
    myData
  }
  
  /**
   * Shuffle 데이터 수신 대기
   * @param timeoutSeconds 대기 시간 (초)
   * @return 성공 여부
   */
  def waitForShuffleData(timeoutSeconds: Long = 30): Boolean = {
    if (expectedShuffleMessages == 0) {
      println(s"[Worker $id] No shuffle messages expected (single worker or all data local)")
      return true
    }
    
    val startTime = System.currentTimeMillis()
    val deadline = startTime + timeoutSeconds * 1000
    
    while (receivedShuffleMessages < expectedShuffleMessages && System.currentTimeMillis() < deadline) {
      Thread.sleep(100)
    }
    
    val success = receivedShuffleMessages >= expectedShuffleMessages
    if (!success) {
      println(s"[Worker $id] Timeout: received $receivedShuffleMessages/$expectedShuffleMessages shuffle messages")
    }
    success
  }
  
  /**
   * 현재 키 범위 가져오기
   */
  def getKeyRanges: Seq[KeyRange] = keyRanges
  
  /**
   * 3단계: 데이터 정렬
   */
  def runSort(): Seq[Long] = {
    println(s"[Worker $id] Starting data sort...")
    val sorted = sorter.getSortedData
    println(s"[Worker $id] Sort complete: ${sorted.size} items")
    sorted
  }
  
  /**
   * 정렬된 데이터 가져오기
   */
  def getSortedData: Seq[Long] = sorter.getSortedData
  
  /**
   * 정렬된 데이터를 파일로 저장
   */
  def saveSortedData(outputPath: String): Unit = {
    sorter.saveToFile(outputPath)
  }
  
  /**
   * 정렬된 데이터를 마스터에게 전송 (Merge를 위해)
   * 메시지 형식: "SORTED:workerId:count:chunk1,chunk2,..."
   */
  def sendSortedDataToMaster(): Unit = {
    val sorted = sorter.getSortedData
    if (sorted.nonEmpty) {
      // 큰 데이터는 청크로 나누어 전송
      val chunkSize = 1000
      sorted.grouped(chunkSize).zipWithIndex.foreach { case (chunk, idx) =>
        val message = s"SORTED:$id:${sorted.size}:${idx}:${chunk.mkString(",")}"
        net.send_to_master(message)
      }
      println(s"[Worker $id] Sent sorted data to Master for merging")
    }
  }
}