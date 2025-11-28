# Software Designing Final Project  
2025 Fall CSED332

## Index
[Members](#Members)  
[Weekly Progress](#Weekly-Progress)  
[MileStones](#MileStones)  
[Feedbacks](#feedbacks)  

## Members
신지용 [JiyongShin](https://github.com/JiyongPostech)  
안강현 [GanghyeonAn](https://github.com/gimon0330)  
박수민 [SuminPark](https://github.com/parksumin1017)  

## Weekly Progress
### Week 1
| Member | Task |
| --- | --- |
| ALL | - Scheduling meetings and deciding on the communication platform<br> - Setting Milestones|
| GanghyeonAn | - Starting off github repository<br> - Studying Parallel Programming, Distrubuted sorting |
| JiyongShin | - Studying Important Libraries such as protobuf |
| SuminPark | - Studying Important Libraries such as gRPC — Summerize pros and cons |

### Week 2 (Midterm week)
### Week 3
| Member | Task |
| --- | --- |
| ALL | - Scheduling meetings and deciding on the communication platform<br> - confirm Milestones|
| GanghyeonAn | - Generate input data using genSort<br> - standardize development environment |
| JiyongShin | - Setup Network<br> - Implement master-worker server and client<br> - Implement simplest possible communication |
| SuminPark | - Studying Important Libraries and summerize usage |
  
Milestone #1: 100% completed. (Concept learning, environment setup, data generation, and initial Master-Worker setup are fully done.)
### Week 4
| Member | Task | Next Steps |
| --- | --- | --- |
| ALL | - Finalized overall Master-Worker based distributed sort architecture<br> - Defined end-to-end pipeline (Sampling → Partitioning → Shuffle → Merge → Output)<br> - Listed core modules and rough interfaces | - Start implementing project skeleton based on design<br> - Implement minimal Master-Worker communication test<br> - Prepare genSort-based test environment |
| GanghyeonAn | - Designed overall data pipeline and key range / pivot strategy<br> - Outlined Worker-side local sort / partition / merge logic | - Implement Worker-side sorting & partitioning skeleton<br> - Connect Worker logic to Master-assigned key ranges |
| JiyongShin | - Designed Master-Worker communication flow (register, status, basic messaging)<br> - Sketched message formats and interfaces | - Implement basic Master/Worker connection & heartbeat<br> - Test multi-Worker connections and simple command flow |
| SuminPark | - Designed high-level system architecture and module boundaries<br> - Drafted skeleton structure (packages, main classes) | - Apply skeleton structure to repo<br> - Finalize shared interfaces so others can plug in code |  
  
Milestone #2: Approximately 50–60% completed. (Basic communication design and partial implementation in progress.)  
Milestone #3: Approximately 30% completed. (System architecture and skeleton code largely defined and partially implemented.)
### Week 5 Progress slides deadline (Nov 16 Sunday, 11:59pm)
[Week5](https://github.com/JiyongPostech/332project/blob/main/document/week5.md)

### Week 6 Progress presentation
| Member | Task | Next Steps |
| --- | --- | --- |
| ALL | - Gave the midterm presentation (pipeline, reliability, logs)<br> - Revised architecture: clearer Master/Worker roles, async shuffle+sort, cleaner interfaces<br> - Small refactors to align modules and messages | - Consolidate the revised design into the code skeleton<br> - Run 2–3-worker smoke tests with structured logs<br> - Track risks (skew, memory, port conflicts) and basic metrics |
| GanghyeonAn | - Composed presentation storyline and examples (end-to-end flow, logs)<br> - Reviewed data path (sampling → partition → shuffle → merge) and module boundaries<br> - Prepared sample I/O cases for demos | - Update run scripts and docs for multi-worker runs<br> - Assemble a small smoke-test set and log-collection guide |
| JiyongShin | - Refined Netty reliability path: SendQueue/ReceiveQueue, UnAckedMap, exponential backoff<br> - Formalized completion based on expectedWorkerIds; improved master event logging | - Standardize handling/logs for ACK loss/delay/dup<br> - Fault-injection (pause/kill worker) to validate retries and recovery<br> - Add trace IDs to retransmit paths |
| SuminPark | - Re-checked sorting/bucketing on Long keys with `[lo, hi)` edges and empty-partition cases<br> - Simplified block-meta schema for merge inputs (size/count/checksum) | - Implement key–value split with value-carrying sort/bucket<br> - Add merge-input validators (order, counts, checksum) and unit tests |

Milestone #2: ~85–90% (multi-worker comms design solid; partial reliability in place)  
Milestone #3: ~60–70% (architecture refined; skeleton/interfaces aligned)  
Milestone #4: ~30–40% (local sort/bucket path defined; KV split next)


### Week 7
| Member | Task | Next Steps |
| --- | --- | --- |
| ALL | - Master–Worker–Worker 전체 파이프라인을 코드 레벨에서 한 번에 연결<br>  (Sampling → Pivot 계산 → Range 브로드캐스트 → Shuffle → Local External Sort → Final partition 출력)<br> - MakeData/Record 포맷을 기준으로 워커 입출력 경로를 정리하고, 멀티 워커 환경에서 간단한 스모크 테스트 진행 | - `MakeData` → `master + workers` → `ValidateSort` 까지 한 번에 돌아가는 통합 스크립트/문서 작성<br> - 2→3→4 workers, input 크기 증가시키면서 성능/메모리/로그 점검<br> - 실패 케이스(잘못된 인자, 빈 디렉터리, 포트 충돌 등) 확인 및 기본 예외 처리 |
| GanghyeonAn | - 기존에 설계했던 데이터 파이프라인과 key-range/pivot 전략을 실제 구현과 맞게 정리<br> - 워커가 Record(100B) 단위로 입력 데이터에서 key(앞 10B)를 추출해 샘플을 만들고, Master가 준 pivot에 따라 타겟 워커를 결정하는 전체 흐름 리뷰<br> - `MakeData`, `InspectData`, `ValidateSort` 유틸과 Worker/Master 코드 간 인터페이스 정합성 점검 | - README/문서에 “end-to-end 실행 시나리오” 정리 (어떤 디렉터리에 어떤 파일이 생성되는지, partition.* 의미 등)<br> - 샘플링 전략(각 파일당 1000개 샘플 등)과 pivot 개수에 대한 튜닝/설명 추가<br> - 최종 결과 검증 플로우 (`ValidateSort` + `InspectData` 사용법) 문서화 및 예시 로그 정리 |
| JiyongShin | - `NettyImplementation`을 기반으로 Master–Worker 등록/피어 목록/샘플 전송/Range 브로드캐스트/FIN/DONE/ALL_DONE 전체 메시지 플로우를 WorkerRuntime에 실제로 연결<br> - Worker 간 Shuffle 구간에서 UDP + 재전송 레이어(sendQueue, unAckedMessages)를 사용하도록 연동하고, 기본적인 ACK/재전송이 동작하는지 확인<br> - Master가 모든 Worker의 REGISTER, SAMPLE, DONE, ALL_DONE을 기준으로 정렬 파이프라인을 관리하는 이벤트 플로우 정리 | - ACK 유실/지연/중복 상황에 대한 로깅 포맷 정리 및 디버깅 편한 로그 추가<br> - 워커 일시 중단/kill 같은 간단한 fault-injection 시나리오 설계 (실제 테스트는 ALL로 진행)<br> - Netty 레벨 예외(연결 끊김, 포트 사용 불가 등)에 대한 기본 처리와 에러 메시지 정리 |
| SuminPark | - Worker 쪽 핵심 구현 완료: <br>   • `DataSorter`: 메모리 버퍼(5만 레코드 단위)를 정렬해 temp 파일(`run_*.dat`)로 flush하는 run 생성기 구현<br>   • `DiskMerger`: 여러 run 파일을 priority queue 기반 k-way merge로 하나의 정렬된 partition 파일로 병합 구현<br>   • `FileIO`: Record(100B)를 정확히 읽고/쓰는 공통 IO 유틸 구현<br>   • `WorkerRuntime`:<br>      - 입력 디렉터리에서 샘플(각 파일당 최대 1000개)을 모아 Master에 전송<br>      - Master로부터 RANGE(pivot)를 수신 후, 다시 입력을 스캔하면서 `getTargetWorker`로 타겟 워커를 결정해 실시간 Shuffle 수행<br>      - 다른 워커에서 오는 레코드는 `DataSorter`에 넣어 external sort용 run 파일로 저장<br>      - 모든 워커의 FIN을 받은 뒤 `DataSorter.close()` → `DiskMerger.merge()`로 최종 `partition.<id>` 생성 후 Master에 DONE 보고<br>   • `worker.Launcher`: CLI 인자(`worker <id> <masterHost> <masterPort> -I <input...> -O <output>`) 파싱 및 WorkerRuntime 부트스트랩 구현 | - `DataSorter`의 버퍼 크기, 임시 파일 수 등 튜닝 포인트 정리(메모리 vs. 임시 파일 trade-off 설명)<br> - Worker 쪽 예외 처리(입력 디렉터리 없음, output 디렉터리 생성 실패, temp 파일 정리 등) 보완<br> - small / medium / large 데이터 세트에서 Worker 단위 성능/메모리 사용 패턴 측정 후 정리 (ALL과 함께 테스트) |

Milestone #3: 100%: 설계된 파이프라인이 대부분 코드로 반영, 나머지는 문서화/정리 수준  
Milestone #4: 90%: local sort, partitioning, disk merge, worker 측 네트워크 역할 구현 완료, 튜닝·에러 처리·간단 테스트가 남아 있음  
Milestone #5: 80%: Master의 샘플 수집 → pivot 계산 → RANGE 배포 → DONE/ALL_DONE 관리까지 구현 end-to-end 스크립트, 다양한 설정으로 통합 테스트가 필요  
Milestone #6: 20%: 기본 유틸(ValidateSort, InspectData)은 준비 완료 체계적인 테스트 플랜, fault 시나리오, 결과 정리 작업이 본격 과제  

### Week 8 Project deadline (Dec 7 Sunday, 11:59pm)
### Week 9 Final presentation

## Milestones
### MileStone #1

- Generate input data [genSort](https://www.ordinal.com/gensort.html)
- Learn about
    - Distributed Sorting (External Sort)
    - Parallel programming
    - Network Libraries (gRPC, Netty)
- Standardize development environment (build tools, scala, etc. )
- Execute master
- Make workers connecting to master

### Milestone #2 — Network Setup

- Finalize the choice of network library for Master-Worker communication
- Implement the simplest possible communication (e.g., “Hello World”) between Master server and Worker client
- Verify that multiple Workers can connect to the Master and that the Master recognizes all connected Workers

### Milestone #3 — System Architecture setup

- Design the overall system structure based on the Master-Worker model
- Design the complete data processing pipeline (e.g., Sampling → Partitioning → Shuffle → Merge)
- Define a list of main classes and functions to be implemented
- Write skeleton code for the defined classes and functions

### **Milestone #4 — Worker Implementation**

- **Local Processing Functions:**
    - Implement **Local Sorting**: read file from local disk and sort it in memory
    - Implement **Partitioning**: divide sorted data according to partition keys
    - Implement **Disk-based Merge**: merge partition files received from other Workers
- **Network Communication (Client Role):**
    - Implement network code for sending sample data and status reports to the Master
    - Implement network code for **Shuffle**, transferring partition data between Workers

### **Milestone #5 — Master Implementation and System Integration**

- **Core Logic Functions:**
    - Implement sorting of sample data collected from Workers and compute global pivot keys
    - Use the computed pivots to determine and assign key ranges for each Worker
- **Network Communication (Server Role):**
    - Implement the Master server code to handle Worker connection requests, data reception, and status updates
- **System Integration:**
    - Integrate all components so that the full sorting pipeline runs from start to finish under Master coordination

### **Milestone #6: Testing, Debugging, and Fault Tolerance Verification**

- Run the full system and verify that the final output file is correctly sorted
- **Fault-Tolerance Test:** forcibly terminate one Worker during execution and confirm that the system successfully completes the job
- Fix bugs found during testing and stabilize the codebase

## Feedbacks
