#!/usr/bin/env python3
"""Watch a long-running task and deliver observations to its originating Codex chat."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import socket
import subprocess
import sys
import time

try:
    import websocket
except ImportError:
    sys.exit("Use a Python interpreter with the websocket-client package installed.")


TERMINAL = {
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL",
    "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT",
}
SIGNALS = {
    "out_of_memory": re.compile(
        rb"\b(?:out of memory|out_of_memory|oom[_ -]kill(?:er|ed)?|CUDA_ERROR_OUT_OF_MEMORY)\b",
        re.IGNORECASE,
    ),
    "nonfinite_loss": re.compile(
        rb"\b(?:\w+_)?loss\b[ \t]*[\"']?[ \t]*(?:[:=][ \t]*)?"
        rb"(?:tensor\([ \t]*)?[+-]?(?:nan|inf(?:inity)?)\b",
        re.IGNORECASE,
    ),
}
STATE_ROOT = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
STATE_ROOT /= "codex-monitor"
RPC_TIMEOUT = 15


class AppServer:
    """Connect to the existing App Server over its local WebSocket."""

    def __init__(self, socket_path):
        codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
        self.path = socket_path or codex_home / "app-server-control/app-server-control.sock"
        self.request_id = 0

    def __enter__(self):
        self.stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.stream.settimeout(RPC_TIMEOUT)
            self.stream.connect(str(self.path))
            self.connection = websocket.create_connection(
                "ws://localhost/", socket=self.stream, suppress_origin=True,
                timeout=RPC_TIMEOUT,
            )
            self.call("initialize", {
                "clientInfo": {
                    "name": "codex-monitor",
                    "title": "Codex Monitor",
                    "version": "1.0.0",
                },
            })
            self.send({"method": "initialized"})
        except (OSError, RuntimeError, ValueError, websocket.WebSocketException):
            self.stream.close()
            raise
        return self

    def __exit__(self, *_):
        self.connection.shutdown()

    def send(self, message):
        self.connection.send(json.dumps(message))

    def call(self, method, params):
        self.request_id += 1
        self.send({"id": self.request_id, "method": method, "params": params})
        deadline = time.monotonic() + RPC_TIMEOUT
        while (remaining := deadline - time.monotonic()) > 0:
            self.connection.settimeout(remaining)
            try:
                raw = self.connection.recv()
            except websocket.WebSocketTimeoutException:
                break
            if not raw:
                raise RuntimeError(f"App Server closed the connection during {method}")
            message = json.loads(raw)
            if message.get("id") != self.request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"{method}: {message['error']['message']}")
            return message["result"]
        raise RuntimeError(f"No App Server acknowledgement for {method}")


def save_state(directory, state):
    temporary = directory / "state.json.new"
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temporary.replace(directory / "state.json")


def emit(args, state, kind, *, key=None, terminal=False, **details):
    key = key or kind
    if key in state["emitted"]:
        return
    state["emitted"].append(key)
    state["pending"].append({
        "event_id": f"{args.run_key}:{key}",
        "event": kind,
        "task_id": args.task_id,
        "source": args.command,
        "terminal": terminal,
        **details,
    })
    state["terminal"] |= terminal


def deliver(args, event):
    with AppServer(args.socket) as server:
        params = {"threadId": args.thread}
        thread = server.call("thread/read", params)["thread"]
        if thread["status"]["type"] == "notLoaded":
            server.call("thread/resume", params)
        server.call("turn/start", {
            **params,
            "input": [],
            "toolOutput": {"name": "codex_monitor", "output": json.dumps(event, ensure_ascii=False)},
        })


def run_on_host(host, command):
    if host:
        command = [
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
            host, shlex.join(command),
        ]
    return subprocess.run(command, capture_output=True, timeout=20)


def log_signals(paths, patterns, host=None):
    fingerprints = {}
    signals = set()
    for path in paths:
        try:
            result = run_on_host(host, ["tail", "-c", "65536", "--", path])
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            fingerprints[path] = hashlib.sha256(result.stdout).hexdigest()
            signals.update(name for name, pattern in patterns.items() if pattern.search(result.stdout))
    return fingerprints, signals


def process_identity(pid):
    result = run_on_host(None, ["ps", "-o", "lstart=", "-o", "stat=", "-p", str(pid)])
    output = result.stdout.decode().strip()
    if result.returncode == 1 and not output and not result.stderr:
        return None
    if result.returncode != 0:
        raise RuntimeError(f"Unable to query PID {pid} (ps exited {result.returncode})")
    start, _, flags = output.rpartition(" ")
    return start.strip() if start and not flags.startswith("Z") else None


class ProcessSource:
    def __init__(self, args, directory, state):
        self.args = args
        self.state = state
        self.process = None
        self.paths = args.log.copy()
        if args.command == "run" and not state["terminal"]:
            output = directory / "task.log"
            self.paths.insert(0, str(output))
            if "launch_requested" not in state:
                state["launch_requested"] = True
                save_state(directory, state)
                try:
                    with output.open("ab") as log:
                        self.process = subprocess.Popen(
                            args.argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                            start_new_session=True,
                        )
                except OSError as error:
                    emit(args, state, "launch_failed", key="_launch_failed",
                         terminal=True, detail=str(error))
                else:
                    state["pid"] = self.process.pid
                    save_state(directory, state)
                    state["pid_identity"] = process_identity(self.process.pid)
                save_state(directory, state)

    def poll(self):
        args = self.args
        state = self.state
        pid = state.get("pid")
        if args.command == "run" and not pid:
            emit(args, state, "launch_unconfirmed", key="_launch_unconfirmed", terminal=True,
                 log_paths=self.paths)
            return
        exit_code = self.process.poll() if self.process else None
        if exit_code is not None:
            ended = True
        elif self.process:
            ended = False
        elif args.command == "attach":
            ended = bool(pid) and (
                not state["pid_identity"] or process_identity(pid) != state["pid_identity"]
            )
        else:
            ended = not state.get("pid_identity") or process_identity(pid) != state["pid_identity"]
        _, signals = log_signals(self.paths, args.patterns)
        for signal in sorted(signals):
            if signal != args.done:
                emit(args, state, signal, key=f"log:{signal}", pid=pid, log_paths=self.paths)
        if ended:
            emit(args, state, "process_exited", key="_process_exited", terminal=True,
                 pid=pid, exit_code=exit_code, log_paths=self.paths, log_signals=sorted(signals))
        elif args.done in signals:
            emit(args, state, args.done, key=f"log:{args.done}", terminal=True,
                 pid=pid, log_paths=self.paths)

    def close(self):
        # Stopping a monitor leaves the task running.
        if self.process:
            self.process.poll()


class StreamSource:
    def __init__(self, args, directory, state):
        self.args = args
        self.state = state
        self.process = None
        self.selector = selectors.DefaultSelector()
        self.buffer = bytearray()
        if state["terminal"]:
            return
        try:
            self.process = subprocess.Popen(
                args.argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as error:
            emit(args, state, "observer_launch_failed", terminal=True, detail=str(error))
        else:
            self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def poll(self):
        if not self.selector.select(0):
            return
        data = os.read(self.process.stdout.fileno(), 65536)
        self.buffer.extend(data)
        if len(self.buffer) > 131072 and b"\n" not in self.buffer:
            emit(self.args, self.state, "observer_invalid_output", terminal=True,
                 detail="The observer exceeded the JSON line size limit.")
            return
        while b"\n" in self.buffer or (not data and self.buffer):
            line, _, self.buffer = self.buffer.partition(b"\n")
            try:
                event = json.loads(line)
                kind = event["event"]
                key = event.get("id", kind)
                if not all(isinstance(value, str) and re.fullmatch(r"[\w.-]{1,120}", value)
                           for value in (kind, key)):
                    raise ValueError("Expected short identifiers for event and id")
                terminal = event.get("terminal", False)
                if not isinstance(terminal, bool):
                    raise ValueError("Expected a boolean terminal field")
                details = {
                    name: event[name][:1000] for name in ("status", "detail")
                    if isinstance(event.get(name), str)
                }
                emit(self.args, self.state, kind, key=f"stream:{key}", terminal=terminal, **details)
                if self.state["terminal"]:
                    break
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                print(f"Ignored malformed observer event ({type(error).__name__})", file=sys.stderr,
                      flush=True)
        if not data and not self.state["terminal"]:
            self.selector.unregister(self.process.stdout)
            emit(self.args, self.state, "observer_exited", key="_observer_exited", terminal=True,
                 observer_exit_code=self.process.poll())

    def close(self):
        self.selector.close()
        if self.process:
            self.process.stdout.close()
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()


class SlurmSource:
    def __init__(self, args, directory, state):
        self.args = args
        self.state = state
        self.absent = 0
        self.fingerprints = {}
        self.last_progress = time.monotonic()
        self.details = {"job_id": args.job_id, "host": args.host, "log_paths": args.log}

    def status(self):
        args = self.args
        queued = run_on_host(args.host, [
            "squeue", "--noheader", "--jobs", args.job_id, "--format", "%i|%T",
        ])
        for line in queued.stdout.decode(errors="replace").splitlines():
            job, _, status = line.partition("|")
            if queued.returncode == 0 and job == args.job_id and status:
                return status.strip(), None
        accounting = run_on_host(args.host, [
            "sacct", "--noheader", "--allocations", "--parsable2",
            "--jobs", args.job_id, "--format", "JobID%64,State%40,ExitCode",
        ])
        for line in reversed(accounting.stdout.decode(errors="replace").splitlines()):
            fields = line.split("|")
            if accounting.returncode == 0 and len(fields) == 3 and fields[0] == args.job_id:
                status = fields[1].split()
                if status:
                    return status[0].rstrip("+"), fields[2]
        return None, None

    def poll(self):
        args = self.args
        state = self.state
        try:
            status, exit_code = self.status()
        except (OSError, subprocess.TimeoutExpired) as error:
            print(f"Slurm query failed: {error}", file=sys.stderr, flush=True)
            status, exit_code = None, None
        if status is None:
            self.absent += 1
            if self.absent >= 5:
                emit(args, state, "scheduler_unavailable", **self.details)
            return
        self.absent = 0
        if status != "RUNNING" or state.get("slurm_state") != "RUNNING":
            self.last_progress = time.monotonic()
        state["slurm_state"] = status
        if status not in TERMINAL | {"RUNNING", "COMPLETING"}:
            return
        current, signals = log_signals(args.log, {**SIGNALS, **args.patterns}, args.host)
        if any(self.fingerprints.get(path) != digest for path, digest in current.items()):
            self.last_progress = time.monotonic()
        self.fingerprints.update(current)
        details = {**self.details, "slurm_state": status}
        if status in TERMINAL:
            emit(args, state, "terminal", key="_slurm_terminal", terminal=True, exit_code=exit_code,
                 log_signals=sorted(signals), **details)
            return
        if status == "RUNNING":
            emit(args, state, "running", **details)
        for signal in sorted(signals):
            emit(args, state, signal, key=f"log:{signal}", **details)
        if (
            status == "RUNNING" and current and args.stall_minutes
            and time.monotonic() - self.last_progress >= args.stall_minutes * 60
        ):
            emit(args, state, "no_log_progress", minutes=args.stall_minutes, **details)

    def close(self):
        pass


def run_worker(args, directory, state):
    stop = directory / "stop"
    source = args.source(args, directory, state)
    try:
        state["watcher"] = "watching"
        save_state(directory, state)
        while not stop.exists():
            if not state["terminal"]:
                try:
                    source.poll()
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    emit(args, state, "monitor_error", key="_monitor_error", detail=str(error))
                save_state(directory, state)
            while state["pending"] and not stop.exists():
                try:
                    deliver(args, state["pending"][0])
                except (OSError, RuntimeError, ValueError, websocket.WebSocketException) as error:
                    state["delivery_error"] = str(error)
                    print(f"Event delivery failed: {error}", file=sys.stderr, flush=True)
                    save_state(directory, state)
                    break
                state["pending"].pop(0)
                state.pop("delivery_error", None)
                save_state(directory, state)
            if state["terminal"] and not state["pending"]:
                state["watcher"] = "finished"
                save_state(directory, state)
                return
            for _ in range(args.interval):
                if stop.exists():
                    break
                time.sleep(1)
        state["watcher"] = "stopped"
        save_state(directory, state)
    finally:
        source.close()


def launch(args):
    args.run_key = hashlib.sha256(
        json.dumps([args.thread, args.command, args.task_id, getattr(args, "host", None)]).encode()
    ).hexdigest()[:20]
    directory = STATE_ROOT / args.run_key
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    state_file = directory / "state.json"
    if args.worker_fd is not None:
        with os.fdopen(args.worker_fd, "a"):
            if not (directory / "stop").exists():
                run_worker(args, directory, json.loads(state_file.read_text()))
        return
    with (directory / "watch.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"Monitor already running. State directory: {directory}")
            return
        if (directory / "stop").exists():
            raise RuntimeError(f"Remove {directory / 'stop'} to restart monitoring")
        if state_file.exists():
            state = json.loads(state_file.read_text())
        else:
            state = {"emitted": [], "pending": [], "terminal": False}
            if args.command == "attach" and args.pid:
                state["pid"] = args.pid
                state["pid_identity"] = process_identity(args.pid)
                if not state["pid_identity"]:
                    raise RuntimeError(f"No running process found for PID {args.pid}")
            save_state(directory, state)
        if state.get("watcher") == "finished":
            print(f"Monitor already finished. State directory: {directory}")
            return
        if args.foreground:
            run_worker(args, directory, state)
            return
        with (directory / "monitor.log").open("ab") as log:
            worker = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--worker-fd", str(lock.fileno()),
                 *sys.argv[1:]],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                start_new_session=True, pass_fds=(lock.fileno(),),
            )
        print(f"Monitor started (PID {worker.pid}). State directory: {directory}")


def parse_args(parser):
    parser.add_argument("--worker-fd", type=int, help=argparse.SUPPRESS)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="verify read access to the originating thread")
    attach = commands.add_parser("attach", help="monitor an existing local PID or log")
    run = commands.add_parser("run", help="launch a local command and monitor its exit")
    stream = commands.add_parser("stream", help="relay JSONL events emitted by an observer")
    slurm = commands.add_parser("slurm", aliases=["start"], help="monitor an existing Slurm job")
    attach.set_defaults(source=ProcessSource)
    run.set_defaults(source=ProcessSource, done=None)
    stream.set_defaults(source=StreamSource)
    slurm.set_defaults(source=SlurmSource, command="slurm")
    for command in (check, attach, run, stream, slurm):
        command.add_argument("--thread", default=os.environ.get("CODEX_THREAD_ID"))
        command.add_argument("--socket", help="path to a non-default App Server control socket")
    for command in (attach, run, stream, slurm):
        command.add_argument("--id", dest="task_id", required=command is not slurm)
        command.add_argument("--interval", type=int, default=30 if command is slurm else 2)
        command.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    for command in (attach, run, slurm):
        command.add_argument("--log", action="append", default=[], help="absolute log path")
        command.add_argument("--match", action="append", default=[], metavar="EVENT=REGEX")
    for command in (run, stream):
        command.add_argument("argv", nargs=argparse.REMAINDER, help="command and arguments after --")
    attach.add_argument("--pid", type=int, help="PID of a local process")
    attach.add_argument("--done", metavar="EVENT", help="stop after this named match")
    location = slurm.add_mutually_exclusive_group(required=True)
    location.add_argument("--host", help="SSH alias for the Slurm host")
    location.add_argument("--local", action="store_true", help="run Slurm commands locally")
    slurm.add_argument("--job-id", required=True)
    slurm.add_argument("--stall-minutes", type=int)
    args = parser.parse_args()
    if not args.thread:
        parser.error("Pass --thread or run from the originating chat")
    if args.command == "check":
        return args
    if args.interval < 1:
        parser.error("--interval must be positive")
    if args.command in {"run", "stream"}:
        if args.argv[:1] == ["--"]:
            args.argv.pop(0)
        if not args.argv:
            parser.error("A command and its arguments are required after --")
    args.patterns = {}
    for pattern in getattr(args, "match", []):
        event, separator, expression = pattern.partition("=")
        if not separator or not expression or not re.fullmatch(r"[\w.-]{1,120}", event):
            parser.error("--match must be EVENT=REGEX")
        try:
            args.patterns[event] = re.compile(expression.encode(), re.MULTILINE)
        except re.error as error:
            parser.error(f"Invalid --match pattern: {error}")
    if any(not Path(path).is_absolute() for path in getattr(args, "log", [])):
        parser.error("--log paths must be absolute")
    if args.command == "attach":
        if args.pid is not None and args.pid < 1:
            parser.error("--pid must be positive")
        if not args.pid and not (args.log and args.patterns):
            parser.error("Provide --pid or a log and at least one --match")
        if args.done and args.done not in args.patterns:
            parser.error("--done must name an existing --match")
    if args.command == "slurm":
        if not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", args.job_id):
            parser.error("--job-id needs a Slurm job ID or a single array task ID")
        if args.host and (args.host.startswith("-") or any(char.isspace() for char in args.host)):
            parser.error("--host needs an SSH alias or destination")
        if args.stall_minutes is not None and (args.stall_minutes < 1 or not args.log):
            parser.error("--stall-minutes requires a positive value and at least one --log")
        args.task_id = args.task_id or args.job_id
    return args


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = parse_args(parser)
    try:
        if args.command == "check":
            with AppServer(args.socket) as server:
                server.call("thread/read", {"threadId": args.thread})
            print("Originating chat is reachable through the Codex App Server.")
        else:
            launch(args)
    except (OSError, RuntimeError, ValueError, websocket.WebSocketException) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
