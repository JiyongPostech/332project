package worker

import network.NettyImplementation
import common.Logger
import java.io.File
import scala.util.Random

object Launcher {
  def main(args: Array[String]): Unit = {
    // 워커 로그 파일 시작
    Logger.init("worker.log")
    Logger.info(s"Worker started with args: ${args.mkString(" ")}")

    if (args.length < 4) {
      println("Usage: worker <masterIP:port> -I <input directory> ... -O <output directory>")
      return
    }

    val masterAddress = args(0).split(":")
    if (masterAddress.length != 2) {
      println("Error: Master address must be in format <IP>:<Port>")
      return
    }
    val masterHost = masterAddress(0)
    val masterPort = masterAddress(1).toInt

    var inputDirs = Seq[File]()
    var outputDir: File = null
    
    var i = 1
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
          if (i < args.length) {
            outputDir = new File(args(i))
            i += 1
          }
        case _ => i += 1
      }
    }

    if (inputDirs.isEmpty || outputDir == null) {
      println("Error: Input(-I) and Output(-O) directories are required.")
      return
    }

    val id = -1
    val myPort = 50000 + Random.nextInt(10000)
    
    Logger.info(s"Starting on port $myPort (Requesting ID from Master...)")
    
    val net = new NettyImplementation(id, myPort)
    val runtime = new WorkerRuntime(id, net, inputDirs, outputDir, 0, masterHost, masterPort)
    
    runtime.start()

    try {
      Thread.currentThread().join()
    } catch {
      case e: InterruptedException => e.printStackTrace()
    } finally {
      Logger.close()
    }
  }
}
