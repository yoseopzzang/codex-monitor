# Codex Monitor

Codex Monitor is a skill for following tasks that keep running after a Codex turn ends. A separate process watches the task and sends useful events back to the chat that set it up. When a build fails or a job finishes, Codex can pick up the investigation or report the result in the same conversation.

## What it can watch

| Task | Events |
| --- | --- |
| A new local command | Exit code and configured matches in captured stdout and stderr |
| An existing process or log | Process exit or configured log matches |
| A Slurm job | Start, completion, out-of-memory errors, non-finite loss, and optional log inactivity |
| An external observer | Events from any tool or script that emits JSON lines |

## Install

Run these commands on the machine that can reach your Codex App Server over its local Unix socket. You need Git and Python 3.8 or newer with `venv`.

```sh
git clone https://github.com/yoseopzzang/codex-monitor.git
cd codex-monitor

mkdir -p "$HOME/.agents/skills"
cp -R ./codex-monitor "$HOME/.agents/skills/"

venv="$HOME/.local/share/codex-monitor/venv"
python3 -m venv "$venv"
"$venv/bin/python" -m pip install -r "$HOME/.agents/skills/codex-monitor/requirements.txt"
```

## Try it in Codex

Start a new task or attach to one that is already running.

- `$codex-monitor Run the test suite in the background and report the failing tests.`
- `$codex-monitor Watch Slurm job 12345 and summarize the result when it finishes.`
- `$codex-monitor Watch the deployment log and let me know if the deployment fails.`

The monitoring host must stay awake and be able to reach the task and the Codex App Server. If event delivery fails, the watcher saves the event locally and retries. To stop watching, create a file named `stop` in the state directory reported by the watcher. The underlying task can keep running.

See the [skill instructions](codex-monitor/SKILL.md) for command options and the JSON event format.
