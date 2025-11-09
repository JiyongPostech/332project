package worker

import common._
import network._

object Launcher {
  def main(args: Array[String]): Unit = {
    // 1) 워커 ID 및 통신 초기화
    val workerId = if (args.nonEmpty) args(0).toInt else 1
    val net: NetworkService = /* TODO: 실제 구현 주입 */ null

    // 2) 실행기(모듈 호출 래퍼) 준비
    val exec = new NoopTaskExecutor() // TODO: 이후 sample/partition/merge/sort 주입

    // 3) 런타임 구성 및 시작
    val runtime = new WorkerRuntime(workerId, net, exec)
    runtime.start()

    // 4) (옵션) 마스터로부터 Assign 수신 시 연결되는 자리
    // net.onAssign { msg => runtime.onAssign(msg) }

    // 5) 종료 대기(데모용)
    // scala.io.StdIn.readLine()
    // runtime.stop()
  }
}
