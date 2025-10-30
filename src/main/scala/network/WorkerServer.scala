package network

import io.grpc.ServerBuilder
import hello.hello._  // ScalaPB가 생성한 패키지 (hello.proto의 package 이름)

import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    val port = 50051
    val server = ServerBuilder
      .forPort(port)
      .addService(WorkerGreeterGrpc.bindService(new WorkerGreeterImpl, ExecutionContext.global))
      .build()

    server.start()
    println(s"Worker gRPC server started on port $port")
    server.awaitTermination()
  }

  // gRPC 서비스 구현부
  class WorkerGreeterImpl extends WorkerGreeterGrpc.WorkerGreeter {
    override def sayHello(request: HelloRequest): Future[HelloReply] = {
      println(s"Received message from master: ${request.message}")
      val response = HelloReply(reply = "Hello received by Worker!")
      Future.successful(response)
    }
  }
}
