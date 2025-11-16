## Week 5
### 1) Milestone-Based Status (Week 4)

- Milestone 1 – Environment & basic run: 100% done  
sbt/Scala standardized; launchers in place; baseline run flow clarified.  
  
- Milestone 2 – Network setup: ~50–60%  
Master–Worker control path framed (registration, peer list sharing, broadcast hook), multi-worker recognition designed. (Reliability/ACK/retry is planned next.)

- Milestone 3 – System architecture: ~30–40%  
End-to-end pipeline (Sampling → Partitioning → Shuffle → Merge) designed; module boundaries and shared interfaces drafted; quantile splitters and [lo, hi) partition rule specified.

- Milestone 4 – Worker implementation: ~30%
Local sorting for Long implemented and prepared for downstream merge; initial partitioning/shuffle interfaces outlined. (Disk-based merge and full receive/reassemble path are next.)
  
### 2) Individual Contributions (Week 4)
#### Ganghyeon An
- Led pipeline and architecture: designed Sampling → Partitioning (quantile splitters, [lo, hi)), Shuffle, and Merge flow.  
- Defined module boundaries and shared interfaces (messages/metadata), and set correctness checks for the merge stage (no gaps/overlaps, count preservation).  
  
#### Jiyong Shin
- Owned the network part: chose Netty and built the Master–Worker control path (registration, peer list exchange, multi-worker recognition) with a broadcast hook for partition plans.  
- Reliability features (ACK/retry/crash handling) were outlined and scheduled for the next iteration.  
  
#### Sumin Park
- Implemented local sorting for Long (Milestone 4 – Local Sorting), including writing sorted outputs in a form consumable by the later merge.  
- Documented usage so the merge stage can plug in cleanly.  
  
#### All
- Finalized the overall distributed-sort architecture and aligned on interfaces.  
- Agreed on quantile-based splitters and the [lo, hi) partition semantics.  
  
### 3) Next Steps
  
- Network reliability & resilience (Milestone 2 → 5): add ACK + bounded retry/backoff and graceful handling of worker disconnect/reconnect; harden broadcasts.  

- Worker data path (Milestone 4): wire partitioning to actual shuffle sends, implement receive-side reassembly, and add disk-based k-way merge.  
  
- Integration (Milestone 5): Master collects results and performs final global k-way merge; verify global ordering and counts.  
  
- Testing (Milestone 6): run an end-to-end smoke with 2–3 workers; introduce a deliberate worker crash to test recovery; add small golden-file checks.  