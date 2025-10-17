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

- Scheduling meetings and deciding on the communication platform

| Member | Task |
| --- | --- |
| ALL |  |
| GanghyeonAn | - Starting off github repository<br>     - Setting Milestones<br> - Studying Parallel Programming, Distrubuted sorting |
| JiyongShin | - Studying Important Libraries such as protobuf |
| SuminPark | - Studying Important Libraries such as gRPC — Summerize pros and cons |

### Week 2 (Midterm week)
### Week 3
### Week 4
### Week 5 Progress slides deadline (Nov 16 Sunday, 11:59pm)
### Week 6 Progress presentation
### Week 7
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
