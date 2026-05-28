import json
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple


LogEntry = Tuple[int, float, str]


@dataclass
class Job:
    id: str
    name: str
    created_at: float = field(default_factory=time.time)
    status: str = "running"  # running|success|error|stopped
    message: str = ""
    result: Any = None

    total: int = 0
    processed: int = 0

    _seq: int = 0
    _logs: Deque[LogEntry] = field(default_factory=lambda: deque(maxlen=4000))
    _cond: threading.Condition = field(default_factory=threading.Condition)
    stop_event: threading.Event = field(default_factory=threading.Event)

    def log(self, message: str):
        msg = (message or "").rstrip("\r\n")
        if not msg:
            return
        with self._cond:
            self._seq += 1
            self._logs.append((self._seq, time.time(), msg))
            self._cond.notify_all()

    def set_progress(self, processed: int, total: int = 0):
        with self._cond:
            self.processed = int(processed or 0)
            if total:
                self.total = int(total)
            self._cond.notify_all()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "job_id": self.id,
            "name": self.name,
            "created_at": self.created_at,
            "status": self.status,
            "message": self.message,
            "processed": self.processed,
            "total": self.total,
        }

    def get_logs_since(self, after_seq: int) -> List[LogEntry]:
        with self._cond:
            return [e for e in list(self._logs) if e[0] > after_seq]

    def wait(self, timeout: float = 2.0):
        with self._cond:
            self._cond.wait(timeout=timeout)


class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()

    def create(self, name: str) -> Job:
        job_id = uuid.uuid4().hex
        job = Job(id=job_id, name=name)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def stop(self, job_id: str) -> bool:
        job = self.get(job_id)
        if not job:
            return False
        job.stop_event.set()
        job.log("Stop requested.")
        return True

    def finish(self, job_id: str, status: str, result: Any = None, message: str = "") -> bool:
        job = self.get(job_id)
        if not job:
            return False
        job.status = status
        job.result = result
        job.message = message or job.message
        job.log(f"Job finished: {status}" + (f" ({job.message})" if job.message else ""))
        return True


jobs = JobManager()


def sse_format(event: str, data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"
