package worker

import network.NettyImplementation
import java.io.File
import java.net.InetSocketAddress

object Launcher {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Usage: worker <id> <masterHost> <masterPort> -I <input> -O <output>")
      return
    }

    val id = args(0).toInt
    val masterHost = args(1)
    val masterPort = args(2).toInt
    
    var inputDirs = Seq[File]()
    var outputDir: File = null
    
    var i = 3
    while (i < args.length) {
      args(i) match {
        case "-I" =>
          i += 1
          while (i < args.length && !args(i).startsWith("-")) {
            inputDirs :+= new File(args(i))
            i += 1
          }
        case "-O" =>
          i += 1
          outputDir = new File(args(i))
          i += 1
        case _ => i += 1
      }
    }
    
    val myPort = 50000 + id
    val net = new NettyImplementation(id, myPort) 
    
    // [수정] masterHost, masterPort 전달
    val runtime = new WorkerRuntime(id, net, inputDirs, outputDir, 0, masterHost, masterPort)
    
    runtime.start()

    try {
      Thread.currentThread().join()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
  }
}