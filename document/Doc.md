# Distributed External Sort (CSED332 Final Project)

Master–Worker 구조로 동작하는 분산 External Sort 시스템입니다.  
랜덤 레코드(100B, 앞 10B = key)를 여러 워커에 나눠 저장한 뒤,

> Sampling → Pivot 계산 → Range 브로드캐스트 → Shuffle → Local External Sort → Merge

순서로 처리하여 최종적으로 각 워커별 `partition.<id>` 파일을 생성합니다.

---

## 1. Prerequisites

- JDK 8 이상  
- Scala 및 빌드 도구 (예: `sbt`)  
- 로컬에서 여러 터미널을 띄울 수 있는 환경  
  (예시에서는 3개의 워커 기준으로 설명)

소스 구조(요약):

- `master/Launcher.scala` – 마스터 진입점 (`master.Launcher`)
- `master/MasterCoordinator.scala` – 샘플 수집, pivot 계산, RANGE 브로드캐스트, DONE/ALL_DONE 관리
- `worker/*` – `WorkerRuntime`, `DataSorter`, `DiskMerger`, `FileIO` 등 워커 쪽 구현
- `network/*` – Netty 기반 Master/Worker/Peer 네트워크 계층
- `common/*` – `Record` 포맷, 메시지 타입 등 공통 부분
- `MakeData.scala`, `InspectData.scala`, `ValidateSort.scala` – 데이터 생성/검사 유틸

---

## 2. 빌드

프로젝트 루트에서 빌드:

```sbt compile```

다른 빌드 툴을 쓴다면, 다음 main object 들을 실행 가능하도록만 설정하면 됩니다.

```
MakeData
master.Launcher
worker.Launcher
ValidateSort
InspectData
```

---

## 3. 입력 데이터 생성

먼저, 테스트에 사용할 입력 데이터를 생성합니다.

```sbt "runMain MakeData"```

실행 후 생성되는 디렉터리/파일:

- 입력
  - `data/input1/file1.dat`, `data/input1/file2.dat`
  - `data/input2/file1.dat`, `data/input2/file2.dat`
  - `data/input3/file1.dat`, `data/input3/file2.dat`
- 출력 디렉터리 (초기에는 비어 있음)
  - `data/output1/`
  - `data/output2/`
  - `data/output3/`

각 `.dat` 파일은 100바이트 레코드 1000개로 구성되어 있고, 워커들이 이 레코드를 읽어서 정렬/분산 처리합니다.

---

## 4. 마스터 실행

### 4.1 마스터 시작

새 터미널(터미널 #1 등)에서:

```sbt "runMain master.Launcher 3"```

설명:

- `3` 은 워커 수 (`number of workers`) 입니다.  
- 마스터는 기본적으로 TCP port `8080` 에 바인드됩니다.

정상적으로 실행되면 예를 들어 다음과 같은 로그가 출력됩니다.

- `[Netty] Master bound to TCP port 8080`  
- `[Master] Waiting for 3 workers to register...`

마스터는 이 상태에서 워커들이 REGISTER 할 때까지 대기합니다.

---

## 5. 워커 실행

워커는 `worker.Launcher` 를 통해 실행됩니다.

### 5.1 워커 실행 인자 형식 (예시)

```worker <id> <masterHost> <masterPort> -I <inputDir1> [-I <inputDir2> ...] -O <outputDir>```

의미:

- `<id>`: 워커 ID (`1`, `2`, `3`, …)
- `<masterHost>`: 마스터가 떠 있는 호스트 (예: `localhost`)
- `<masterPort>`: 마스터 포트 (예: `8080`)
- `-I <inputDir>`: 이 워커가 처리할 입력 디렉터리 (여러 개 지정 가능)
- `-O <outputDir>`: 최종 정렬 결과(`partition.<id>`)를 저장할 출력 디렉터리

### 5.2 예시: 3 워커 로컬 실행

터미널 #2 (Worker 1):

```sbt "runMain worker.Launcher 1 localhost 8080 -I data/input1 -O data/output1"```

터미널 #3 (Worker 2):

```sbt "runMain worker.Launcher 2 localhost 8080 -I data/input2 -O data/output2"```

터미널 #4 (Worker 3):

```sbt "runMain worker.Launcher 3 localhost 8080 -I data/input3 -O data/output3"```

각 워커의 동작 흐름(요약):

1. 마스터에 TCP로 접속 후 `REGISTER` 메시지 전송  
2. 로컬 입력 디렉터리에서 일부 레코드를 샘플링하고, 샘플을 마스터에 `SAMPLE` 메시지로 전송  
3. 마스터가 모든 샘플을 모아서 pivot/범위를 계산한 뒤, `RANGE` 메시지로 브로드캐스트  
4. 받은 범위에 따라, 다른 워커들에게 레코드를 UDP로 Shuffle  
5. 수신한 레코드를 `DataSorter` 를 통해 외부 정렬용 temp run 파일로 저장  
6. 모든 워커 간 Shuffle 종료 후(`FIN` / `finishedPeers` 기준), `DiskMerger` 로 temp run 파일들을 병합하여 `partition.<id>` 생성  
7. 마스터에 `DONE` 메시지로 완료 보고 → 마지막에 `ALL_DONE` 신호를 받으면 종료

마스터 터미널에서는 대략 다음과 같은 흐름의 로그를 볼 수 있습니다.

- Worker 등록 로그  
  - `[Master] Registered worker 1`  
  - `[Master] Registered worker 2`  
  - `[Master] Registered worker 3`  
  - `[Master] All workers registered.`  
- 샘플 수신 및 RANGE 분배  
  - `[Master] Received samples from worker 1`  
  - `[Master] Received samples from worker 2`  
  - `[Master] Received samples from worker 3`  
  - `[Master] Distributing RANGE to workers...`  
- 완료 처리  
  - `[Master] Worker 1 DONE (1/3)`  
  - `[Master] Worker 2 DONE (2/3)`  
  - `[Master] Worker 3 DONE (3/3)`  
  - `[Master] All workers finished. Sending ALL_DONE.`  

---

## 6. 결과 검증

정렬이 끝나면 각 워커의 출력 디렉터리에 다음 파일이 생성됩니다.

- `data/output1/partition.1`
- `data/output2/partition.2`
- `data/output3/partition.3`

### 6.1 정렬 여부 검증 (ValidateSort)

프로젝트 루트에서:

```sbt "runMain ValidateSort"```

각 `partition.*` 파일에 대해 이전 레코드보다 key 가 큰지(오름차순) 검사합니다.  
정상 정렬 상태라면 예를 들어:

- `Checking partition.1...`  
- `Checking partition.2...`  
- `Checking partition.3...`  
- `All files are correctly sorted!`  

와 같은 메시지를 볼 수 있습니다.

### 6.2 결과 키 범위 확인 (InspectData)

```sbt "runMain InspectData"```

각 partition 파일에 대해:

- 파일 크기와 레코드 수  
- 앞 5개 레코드의 key (앞 10바이트) hex 값  
- 마지막 레코드의 key hex 값  

을 출력합니다. 이를 통해:

- `partition.1` < `partition.2` < `partition.3` 순서로 key 범위가 증가하는지  
- 각 partition 내부에서 key 가 비내림차순인지  

눈으로도 확인할 수 있습니다.