// src/main/scala/worker/DataShuffler.scala
package worker

import common.KeyRange
import scala.collection.mutable

/** 데이터 재분배(Shuffle) 모듈 (수정) */
class DataShuffler(
  myWorkerId: Int,
  sendFunction: (Int, String) => Unit  // (targetWorkerId, payload) => Unit
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
        myData += value
      } else {
        dataByWorker.getOrElseUpdate(targetWorker, mutable.ArrayBuffer[Long]()) += value
      }
    }

    // 다른 워커들에게 데이터 전송 (빈 데이터도 전송하여 동기화 보장)
    keyRanges.foreach { range =>
      if (range.workerId != myWorkerId) {
        val values = dataByWorker.getOrElse(range.workerId, mutable.ArrayBuffer.empty)
        
        // (수정) "DATA:" 접두사 제거. 순수 payload (예: "100,200,300") 전송
        val message = values.mkString(",")
        
        sendFunction(range.workerId, message)
        println(s"[Worker $myWorkerId] Sent ${values.size} items to Worker ${range.workerId} (to reliable queue)")
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
   * (수정) 이 함수는 더 이상 WorkerRuntime의 수신 경로에서 사용되지 않습니다.
   * (WorkerRuntime이 자체 parseData 사용)
   */
  def parseReceivedData(message: String): Seq[Long] = {
    if (message.startsWith("DATA:")) {
      val dataStr = message.substring(5)
      if (dataStr.isEmpty) Seq.empty
      else dataStr.split(",").map(_.toLong).toSeq
    } else {
      // 이전 로직과의 호환성을 위해 남겨둠
      if (message.isEmpty) Seq.empty
      else message.split(",").map(_.toLong).toSeq
    }
  }
}