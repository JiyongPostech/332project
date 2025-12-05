package master

import network.NettyImplementation
import common.Logger
import java.net.InetAddress
import scala.util.Random

object Launcher {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: master <# of workers>")
      return
    }

    // 마스터 로그 파일 시작
    Logger.init("master.log")
    Logger.info("Master started.")

    val numWorkers = args(0).toInt
    val myPort = 30000 + Random.nextInt(10000)
    val myIp = InetAddress.getLocalHost.getHostAddress
    
    // [화면 출력] 필수 정보
    println(s"$myIp:$myPort")
    Logger.info(s"Master Address: $myIp:$myPort")

    val net = new NettyImplementation(0, myPort)
    val coord = new MasterCoordinator(net, numWorkers)
    
    net.bind(coord.handleMessage)
    
    try {
      Thread.currentThread().join()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    } finally {
      Logger.close()
    }
  }
}
