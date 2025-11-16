package worker

import common.KeyRange
import scala.collection.mutable

/** 데이터 재분배(Shuffle) 모듈 */
class DataShuffler(
  myWorkerId: Int,
  sendFunction: (Int, String) => Unit  // (targetWorkerId, message) => Unit
) {

  /**
   * 키 범위에 따라 데이터를 적절한 워커에게 재분배
   * @param data 재분배할 데이터
   * @param keyRanges 각 워커가 담당하는 키 범위
   * @return 자신이 담당하는 데이터
   */
  def shuffle(data: Seq[Long], keyRanges: Seq[KeyRange]): Seq[Long] = {
    val myData = mutable.ArrayBuffer[Long]()
    val dataByWorker = mutable.Map[Int, mutable.ArrayBuffer[Long]]()

    // 각 워커별로 데이터 분류
    data.foreach { value =>
      val targetWorker = findTargetWorker(value, keyRanges)

      if (targetWorker == myWorkerId) {
        // 내 데이터는 바로 보관
        myData += value
      } else {
        // 다른 워커의 데이터는 버퍼에 모음
        dataByWorker.getOrElseUpdate(targetWorker, mutable.ArrayBuffer[Long]()) += value
      }
    }

    // 다른 워커들에게 데이터 전송 (빈 데이터도 전송하여 동기화 보장)
    keyRanges.foreach { range =>
      if (range.workerId != myWorkerId) {
        val values = dataByWorker.getOrElse(range.workerId, mutable.ArrayBuffer.empty)
        sendDataToWorker(range.workerId, values.toSeq)
      }
    }

    myData.toSeq
  }

  /**
   * 값이 어느 워커의 범위에 속하는지 찾기
   */
  private def findTargetWorker(value: Long, keyRanges: Seq[KeyRange]): Int = {
    keyRanges.find { range =>
      value >= range.minKey && value <= range.maxKey
    }.map(_.workerId).getOrElse(myWorkerId)  // 기본값은 자신
  }

  /**
   * 특정 워커에게 데이터 전송
   * 데이터 형식: "DATA:value1,value2,value3"
   * 비어있어도 ACK 전송하여 수신 카운팅 보장
   */
  private def sendDataToWorker(targetWorkerId: Int, data: Seq[Long]): Unit = {
    val message = if (data.nonEmpty) {
      s"DATA:${data.mkString(",")}"
    } else {
      "DATA:"  // 빈 데이터도 메시지 전송 (동기화 보장)
    }
    sendFunction(targetWorkerId, message)
    println(s"[Worker $myWorkerId] Sent ${data.size} items to Worker $targetWorkerId")
  }

  /**
   * 수신한 메시지를 파싱하여 데이터 추출
   * @param message "DATA:value1,value2,value3" 형식
   * @return 파싱된 데이터
   */
  def parseReceivedData(message: String): Seq[Long] = {
    if (message.startsWith("DATA:")) {
      val dataStr = message.substring(5)
      if (dataStr.isEmpty) Seq.empty
      else dataStr.split(",").map(_.toLong).toSeq
    } else {
      Seq.empty
    }
  }
}
