package master

import network.NettyImplementation
import java.net.InetSocketAddress
import org.slf4j.LoggerFactory

object Launcher {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: master <number of workers>")
      return
    }
    
    val numWorkers = args(0).toInt
    val port = 8080 
    
    val net = new NettyImplementation(0, port)
    val coord = new MasterCoordinator(net, numWorkers)
    
    coord.start()
    
    // [추가된 부분] 메인 스레드가 종료되지 않도록 대기
    try {
      Thread.currentThread().join()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
  }
}