# 개발 환경 세팅

### 1. 필수 프로그램

* **JDK 17**: [Adoptium 다운로드](https://adoptium.net)
* **sbt 1.11.7**: [sbt 다운로드](https://www.scala-sbt.org/download.html)
* **protoc**: `Build.sbt` 컴파일 시 자동 다운로드
* **Scala**: sbt에서 자동 설치

### 2. 폴더 구조

* 깃에 대강 구조화

### 3. 코드 작성

* `src/main/protobuf/hello.proto`
* `src/main/scala/network/WorkerServer.scala`
* `src/main/scala/network/MasterClient.scala`
* 마찬가지로 깃에 대강의 구현

### 4. 빌드

```bash
cd /깃/경로
sbt compile
```

### 5. 실행

* **터미널 1**:

  ```bash
  sbt "runMain network.WorkerServer"
  ```

  출력: `Worker gRPC server started on port 50051`

* **터미널 2**:

  ```bash
  sbt "runMain network.MasterClient"
  ```

  출력: `[Master] Response from worker: Hello received by Worker!`

---

## gRPC가 뭔가요

* **Google이 개발한 원격 프로시저 호출(Remote Procedure Call, RPC) 프레임워크**
* 네트워크 상의 두 프로그램(예: Master ↔ Worker)이 함수를 호출하듯 통신하게 함
* **프로토콜**: HTTP/2 기반. 고속, 양방향 스트리밍 지원

### 구조

| 구성요소        | 역할                               |
| ----------- | -------------------------------- |
| `.proto 파일` | 통신할 함수(서비스)와 데이터(메시지) 구조 정의      |
| Server      | `.proto`에 정의된 RPC 함수를 "구현"       |
| Client      | `.proto`에 정의된 RPC 함수를 "호출"       |
| Stub        | Client가 사용할 수 있도록 자동 생성된 함수 코드   |
| Channel     | Client가 Server에 연결할 때 사용하는 통신 세션 |

---

## Proto 기본 문법 예시

```proto
syntax = "proto3";  // 프로토콜 버전 (proto3)
package hello;      // 패키지 이름

// 서비스 정의 (gRPC에서 사용할 서비스 이름 및 함수들 정의)
service WorkerGreeter {
  // SayHello 함수 정의
  // HelloRequest 메시지를 받아서 HelloReply 메시지를 반환하는 RPC 함수
  rpc SayHello (HelloRequest) returns (HelloReply);
}

// 요청 메시지 정의 (클라이언트가 보낼 데이터)
message HelloRequest {
  string message = 1;  // 클라이언트가 보낼 메시지 (ex: "Hello Worker!")
}

// 응답 메시지 정의 (서버가 보낼 데이터)
message HelloReply {
  string reply = 1;  // 서버의 응답 메시지 (ex: "Hello received by Worker!")
}
```

* **service Greeter** → gRPC 서비스 이름
* **rpc SayHello(...)** → 클라이언트가 호출할 수 있는 원격 함수
* **message** → Python의 클래스 같은 (전달할 데이터에 대한) 데이터 구조

---

## 서버 구현 예시

```scala
package network

import io.grpc.ServerBuilder   // gRPC 서버 생성기
import hello.hello._          // 프로토파일에서 자동 생성된 서비스와 메시지

import scala.concurrent.{ExecutionContext, Future}  // 비동기 처리 (Future)

// 서버 객체 생성
object WorkerServer {
  def main(args: Array[String]): Unit = {
    // gRPC 서버를 포트 50051로 시작
    val server = ServerBuilder
      .forPort(50051)  // 서버 포트 설정
      .addService(WorkerGreeterGrpc.bindService(new WorkerGreeterImpl, ExecutionContext.global)) // 서비스 등록
      .build()
      .start() // 서버 시작

    println("Server started on port 50051")  // 서버 시작 메시지 출력
    server.awaitTermination()  // 서버가 종료될 때까지 기다림
  }

  // 서비스 구현 (WorkerGreeter)
  class WorkerGreeterImpl extends WorkerGreeterGrpc.WorkerGreeter {
    // SayHello 함수의 구현 (클라이언트가 요청할 때 호출)
    override def sayHello(req: HelloRequest): Future[HelloReply] = {
      // 클라이언트의 요청 메시지 출력
      println(s"Received message: ${req.message}")
      // 서버의 응답 생성 (Future로 반환)
      Future.successful(HelloReply(reply = "Hello received by Worker!"))
    }
  }
}
```

---

## 클라이언트 구현 예시

```scala
package network

import io.grpc.ManagedChannelBuilder  // gRPC 채널 빌더
import hello.hello._                 // 프로토파일에서 자동 생성된 서비스와 메시지

object MasterClient {
  def main(args: Array[String]): Unit = {
    // gRPC 서버와 연결할 채널 생성 (localhost:50051)
    val channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                                       .usePlaintext()  // 암호화 비활성화 (개발용)
                                       .build()

    // 클라이언트 스텁 생성 (WorkerGreeter 서비스의 동기 호출용 스텁)
    val stub = WorkerGreeterGrpc.blockingStub(channel)

    // 클라이언트 요청 생성
    val request = HelloRequest(message = "Hello Worker!")

    // 서버에 요청을 보내고 응답 받기
    val response = stub.sayHello(request)

    // 서버의 응답 출력
    println(s"[Master] Response from worker: ${response.reply}")

    // 채널 종료
    channel.shutdown()
  }
}
```

* **ManagedChannelBuilder**: 서버와 통신할 수 있도록 gRPC 채널을 설정
* **blockingStub**: WorkerGreeter 서비스에 대한 동기적 클라이언트 스텁을 생성. 스텁을 통해 클라이언트는 서버에 요청을 보낼 수 있음
* **stub.sayHello**: `sayHello` RPC 메소드를 호출하고, 서버의 응답을 받음. 이때, 요청 메시지 `HelloRequest`와 응답 메시지 `HelloReply`는 proto 파일에서 정의된 데이터 구조.

---

