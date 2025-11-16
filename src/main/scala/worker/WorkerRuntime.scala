package worker

import common._
import network._
import scala.collection.mutable

/** 워커 프로세스의 생명주기/이벤트 루프 */
class WorkerRuntime(
  id: Int,
  net: NetworkService
) {
  
  // 데이터 처리 모듈
  private val analyzer = new DataAnalyzer()
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
    // P2P 메시지 수신(데이터 평면)은 이후 필요 시 bind 사용
    net.bind(onPeerMessage)
    // 마스터 접속 및 컨트롤 메시지 콜백 연결은 네트워크 계층 확장 시 주입 예정
    net.connect_to_master(onInitialPeers, onPeerJoined, onPeerLeft, onKeyRangeFromMaster, onInitialDataFromMaster)
    // 별도: 등록 메시지 전송(프로토콜은 ControlMessage.RegisterWorker 등으로 통일)
  }

  /** 안전 종료 */
  def stop(): Unit = net.stop()

  /**
   * P2P 메시지 수신 콜백
   * 데이터 타입에 따라 적절한 처리 수행
   * 메시지 형식: "TYPE:DATA"
   * - INITIAL_DATA:value1,value2,... (마스터로부터 초기 데이터)
   * - ANALYZE:value1,value2,... (분석용 데이터)
   * - DATA:value1,value2,... (정렬용 데이터)
   * - KEYRANGE:workerId:minKey:maxKey,... (키 범위 정보)
   */
  private def onPeerMessage(senderId: Int, msg: String): Unit = {
    val parts = msg.split(":", 2)
    if (parts.length < 2) return
    
    parts(0) match {
      case "INITIAL_DATA" =>
        // 마스터로부터 초기 데이터 수신
        val data = parseData(parts(1))
        receivedData ++= data
        initialDataReceived = true
        println(s"[Worker $id] Received ${data.size} initial data items from Master")
        
      case "ANALYZE" =>
        // 분석용 데이터 수신
        val data = parseData(parts(1))
        receivedData ++= data
        println(s"[Worker $id] Received ${data.size} items for analysis from $senderId")
        
      case "DATA" =>
        // 정렬용 데이터 수신 (Shuffle 단계)
        val data = shuffler.parseReceivedData(msg)
        sorter.insertAll(data)
        receivedShuffleMessages += 1
        println(s"[Worker $id] Received ${data.size} items for sorting from $senderId ($receivedShuffleMessages/$expectedShuffleMessages)")
        
        // 모든 shuffle 데이터 수신 완료 확인
        if (expectedShuffleMessages > 0 && receivedShuffleMessages >= expectedShuffleMessages) {
          println(s"[Worker $id] All shuffle data received")
          // 래치는 이미 카운트가 0이면 countDown 호출이 무시됨
        }
        
      case "KEYRANGE" =>
        // 키 범위 정보 수신
        keyRanges = parseKeyRanges(parts(1))
        println(s"[Worker $id] Received key ranges: $keyRanges")
        
      case _ =>
        println(s"[Worker $id] Unknown message type from $senderId: ${parts(0)}")
    }
  }
  
  /**
   * 데이터 파싱 (쉼표로 구분된 숫자)
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
   */
  private def sendData(targetWorkerId: Int, message: String): Unit = {
    net.send(targetWorkerId, message)
  }
  
  /**
   * 리시버가 다른 워커에게 데이터를 전송할 때 사용하는 public 함수
   * @param targetWorkerId 대상 워커 ID
   * @param dataType 데이터 타입 (0: 정렬, 1: 분석)
   * @param data 전송할 데이터
   */
  def sendDataToWorker(targetWorkerId: Int, dataType: Int, data: Seq[Long]): Unit = {
    val typePrefix = if (dataType == 0) "DATA" else "ANALYZE"
    val message = s"$typePrefix:${data.mkString(",")}"
    net.send(targetWorkerId, message)
    println(s"[Worker $id] Sent ${data.size} items (type=$dataType) to Worker $targetWorkerId")
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
