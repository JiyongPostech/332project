package network

import io.grpc.ManagedChannelBuilder
import hello.hello._
import scala.concurrent.Await
import scala.concurrent.duration._

object MasterClient {
  def main(args: Array[String]): Unit = {
    // 워커 서버가 실행 중이어야 함
    val channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build()
    val stub = WorkerGreeterGrpc.blockingStub(channel)

    val request = HelloRequest(message = "Hello Worker!")
    val response = stub.sayHello(request)

    println(s"Response from worker: ${response.reply}")

    channel.shutdown()
  }
}
