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

| Member | Progress | Next Steps |
| --- | --- | --- |
| ALL | Master–Worker 파이프라인을 코드로 거의 끝까지 연결 (샘플링 → RANGE → Shuffle → local sort/merge → partition 출력) | MakeData → master/workers → ValidateSort 까지 한 번에 도는 기본 스크립트/실행 가이드 만들기 |
| GanghyeonAn | Record/MakeData/ValidateSort 기준으로 데이터 포맷·파이프라인 흐름 정리, pivot/partition 전략이 구현과 일치하는지 검토 | README에 전체 데이터 흐름·디렉터리 구조·검증 방법(ValidateSort, InspectData) 간단히 정리 |
| JiyongShin | Netty 네트워크(REGISTER, SAMPLE, RANGE, DONE, ALL_DONE)와 WorkerRuntime을 실제로 연결, UDP 기반 shuffle 경로 연동 | 단계별 로그 포맷 정리해서 “지금 어느 단계인지” 바로 보이게 만들고, 간단한 fault 시나리오(워커 kill 등) 설계 |
| SuminPark | Worker 핵심 로직 구현 완료: DataSorter(외부 정렬용 run 생성), DiskMerger(머지), FileIO, WorkerRuntime(샘플 수집·RANGE 수신·shuffle·정렬·DONE 보고), worker.Launcher 인자 처리 | DataSorter/DiskMerger 파라미터(버퍼 크기 등) 간단 튜닝, 입력/출력 경로 오류에 대한 기본 예외 처리·sanity check 보완 |

Milestone #3: 100%: 설계된 파이프라인이 대부분 코드로 반영, 나머지는 문서화/정리 수준  
Milestone #4: 90%: local sort, partitioning, disk merge, worker 측 네트워크 역할 구현 완료, 튜닝·에러 처리·간단 테스트가 남아 있음  
Milestone #5: 80%: Master의 샘플 수집 → pivot 계산 → RANGE 배포 → DONE/ALL_DONE 관리까지 구현 end-to-end 스크립트, 다양한 설정으로 통합 테스트가 필요  
Milestone #6: 20%: 기본 유틸(ValidateSort, InspectData)은 준비 완료 체계적인 테스트 플랜, fault 시나리오, 결과 정리 작업이 본격 과제  

### Week 8 Project deadline (Dec 7 Sunday, 11:59pm)
### Week 8

| Member | Task | Next Steps |
| --- | --- | --- |
| ALL | Completed an end-to-end script that runs MakeData → master/workers → ValidateSort and wrote a basic execution guide. Verified that the full pipeline works from sampling to final output on small-size datasets only, because server issues prevented running larger-scale experiments.                                                                                                                                                                    | Prepare the final presentation and demo together: finalize slides, polish the live demo flow using the small-size dataset scenario, and rehearse as a team while adjusting timing and roles. |
| GanghyeonAn | Re-checked the actual code flow with Record/MakeData/ValidateSort and confirmed that the implemented pivot/partition strategy matches the design. Updated the README with a concise overview of the data format, full pipeline, directory structure, and validation tools (ValidateSort, InspectData), including simple usage examples.                                                                                                                    | Prepare the final presentation and demo together: finalize slides, polish the live demo flow using the small-size dataset scenario, and rehearse as a team while adjusting timing and roles. |
| JiyongShin  | Finalized integration between the Netty network layer (REGISTER, SAMPLE, RANGE, DONE, ALL_DONE) and WorkerRuntime. Standardized log formats so that the current phase of the pipeline is immediately visible from logs. Designed and executed simple fault scenarios (e.g., killing a worker) and observed the resulting behavior and log patterns.                                                                                                  | Prepare the final presentation and demo together: finalize slides, polish the live demo flow using the small-size dataset scenario, and rehearse as a team while adjusting timing and roles. |
| SuminPark   | Completed worker core logic: `DataSorter` (run generation for external sort), `DiskMerger` (merge), `FileIO`, and `WorkerRuntime` (sample collection, `RANGE` handling, shuffle, local sort, `DONE` reporting), plus `worker.Launcher` argument handling. Tuned key parameters (e.g., buffer sizes) and added basic error handling and sanity checks for input/output paths. Verified correctness on small-size datasets using `ValidateSort` and `InspectData`. | Prepare the final presentation and demo together: finalize slides, polish the live demo flow using the small-size dataset scenario, and rehearse as a team while adjusting timing and roles. |

**Milestone #3: 100%**

* The designed master–worker pipeline is fully reflected in code and scripts. Documentation (README + basic execution guide) is in place, so the design phase is effectively complete.

**Milestone #4: 100%**

* Local sort, partitioning, disk merge, and worker-side networking are fully implemented with parameter tuning and basic error handling. The system is stable enough for small-scale end-to-end runs.

**Milestone #5: 100%**

* Implemented an end-to-end flow from master sampling → pivot computation → `RANGE` distribution → worker `DONE`/`ALL_DONE` handling, wrapped in runnable scripts. End-to-end behavior has been confirmed **on small-size datasets due to server issues**; large-scale stress tests could not be executed.

**Milestone #6: 60%**

* Core utilities (`ValidateSort`, `InspectData`) are in use for correctness checks, and step-by-step logs plus simple fault scenarios have been tested. Remaining work focuses on more systematic performance/scale evaluation (once servers allow larger runs) and organizing these results into final presentation materials (plots, tables, and summaries).


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
