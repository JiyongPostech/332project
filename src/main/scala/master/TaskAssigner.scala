package master

import common._

object TaskAssigner {

  /** 태스크 일괄 배정 (라운드로빈 등 간단 정책: 이후 교체 가능) */
  def assignAll(
    job: Job,
    workers: Seq[WorkerInfo],
    ctl: ControlSender,
    onProgress: (String, String, Int) => Unit,
    onFinish: (String, String, Seq[String]) => Unit,
    onFail: (String, String, String) => Unit
  ): Unit = {
    require(workers.nonEmpty, "No workers to assign tasks.")

    var i = 0
    job.tasks.foreach { t =>
      val w = workers(i % workers.size)
      i += 1
      ctl.sendAssign(w.id, ControlMessage.AssignTask(job.id, t))
      // 워커 측에서 Progress/Finish/Fail이 들어오면 위 콜백으로 연결될 예정
    }
  }
}
