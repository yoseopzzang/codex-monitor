# Codex Monitor

Have Codex follow a task across turns. A background observer sends events to the chat that set it up. It can watch a local command, an existing process or log, a Slurm job, or another event source that emits JSON lines.

## Install

Run these commands from the repository root on the machine that hosts your Codex App Server. You need Python 3.8 or newer with `venv`.

```sh
mkdir -p "$HOME/.agents/skills"
cp -R ./codex-monitor "$HOME/.agents/skills/"

venv="$HOME/.local/share/codex-monitor/venv"
python3 -m venv "$venv"
"$venv/bin/python" -m pip install -r "$HOME/.agents/skills/codex-monitor/requirements.txt"
```

## Use

In Codex, enter `$codex-monitor Run the tests in the background and tell me if they fail.` To watch an existing task, give Codex its PID, log path, or job ID.

The monitoring host must stay awake and be able to reach both the task and the Codex App Server. Each watcher reports its state directory. Create a file named `stop` there to stop watching. The monitored task can keep running.

See [the skill instructions](codex-monitor/SKILL.md) for observer inputs and event handling.
