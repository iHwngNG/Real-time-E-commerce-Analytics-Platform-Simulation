"""
Control Panel — Streamlit Web UI
Purpose: Provide a visual interface to manage the entire analytics platform.
Features:
  - Start/Stop Docker infrastructure with status indicators
  - Run Simulator (seed / simulate / both) with live log output
  - Run Streaming pipeline with live log output
  - Docker container health monitoring
Usage: streamlit run control_panel/app.py
"""

import os
import subprocess
import threading
import time
from collections import deque
from datetime import datetime

import streamlit as st

# =============================================================================
# Configuration
# =============================================================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Max log lines to keep in memory per panel
MAX_LOG_LINES = 500


# =============================================================================
# Session State Initialization
# =============================================================================
def _init_state():
    """Initialize Streamlit session state for process tracking."""
    defaults = {
        "docker_logs": deque(maxlen=MAX_LOG_LINES),
        "simulator_logs": deque(maxlen=MAX_LOG_LINES),
        "streaming_logs": deque(maxlen=MAX_LOG_LINES),
        "docker_running": False,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def _timestamp():
    """Return formatted current timestamp."""
    return datetime.now().strftime("%H:%M:%S")


# =============================================================================
# Process Execution Helpers
# =============================================================================
def _run_command_with_logs(cmd_list: list, log_queue: deque, label: str):
    """
    Run a subprocess and stream its stdout/stderr into a deque.
    """
    # On Windows, strings often work better than lists with shell=True
    cmd_str = " ".join(cmd_list) if isinstance(cmd_list, list) else cmd_list

    log_queue.append(f"[{_timestamp()}] 🚀 Initiating {label}...")
    log_queue.append(f"[{_timestamp()}] 📂 Directory: {PROJECT_ROOT}")
    log_queue.append(f"[{_timestamp()}] 💻 Command: {cmd_str}")

    try:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        proc = subprocess.Popen(
            cmd_str,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            shell=True,
            env=env,
        )

        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if line:
                stripped = line.rstrip()
                if stripped:
                    log_queue.append(f"[{_timestamp()}] {stripped}")

        exit_code = proc.wait()
        if exit_code == 0:
            log_queue.append(f"[{_timestamp()}] ✅ {label} - FINISHED SUCCESSFULLY.")
        else:
            log_queue.append(
                f"[{_timestamp()}] ❌ {label} - FAILED (Exit Code: {exit_code})"
            )

    except Exception as e:
        log_queue.append(f"[{_timestamp()}] ❌ CRITICAL ERROR: {str(e)}")


def _run_in_thread(cmd_list: list, log_queue: deque, label: str):
    """Launch _run_command_with_logs in a daemon thread."""
    t = threading.Thread(
        target=_run_command_with_logs,
        args=(cmd_list, log_queue, label),
        daemon=True,
    )
    t.start()
    return t


# =============================================================================
# Docker Status Helper
# =============================================================================
def _get_container_status() -> list[dict]:
    """
    Run 'docker compose ps' and parse the output into a list of dicts.
    """
    try:
        # Use simple command first to ensure compatibility
        result = subprocess.run(
            'docker compose ps --format "{{.Name}}|{{.Status}}|{{.Ports}}"',
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            shell=True,
        )
        containers = []
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                parts = line.split("|", 2)
                containers.append(
                    {
                        "name": parts[0].strip(),
                        "status": parts[1].strip(),
                        "ports": parts[2].strip() if len(parts) > 2 else "",
                    }
                )
        return containers
    except Exception:
        return []


def _status_icon(status: str) -> str:
    """Map container status to an emoji icon."""
    s = status.lower()
    if "up" in s and "healthy" in s:
        return "🟢"
    if "up" in s:
        return "🟡"
    if "exited" in s and "(0)" in s:
        return "⚪"
    if "exited" in s:
        return "🔴"
    return "⚫"


# =============================================================================
# UI Components
# =============================================================================
def _render_log_panel(logs: deque, title: str, height: int = 400):
    """Render a scrollable log panel styled like a terminal."""
    log_text = "\n".join(logs) if logs else "No logs yet. Press a button to start."
    st.markdown(
        f"""
        <div style="
            background-color: #0e1117; color: #00ff41;
            font-family: monospace; font-size: 11px;
            padding: 10px; border-radius: 5px;
            height: {height}px; overflow-y: auto;
            border: 1px solid #333; white-space: pre-wrap;
        ">
<span style="color: #888;"># {title}</span>

{log_text}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_status_table(containers: list[dict]):
    """Render Docker container status as a table."""
    if not containers:
        st.info("No containers found. Start Docker infrastructure first.")
        return

    rows = []
    for c in containers:
        icon = _status_icon(c["status"])
        name = c["name"].replace("ecommerce_", "")
        rows.append(f"| {icon} | `{name}` | {c['status']} | {c['ports']} |")

    st.markdown("| | Service | Status | Ports |\n|---|---|---|---|\n" + "\n".join(rows))


# =============================================================================
# Main Page
# =============================================================================
def main():
    _init_state()
    st.set_page_config(
        page_title="Analytics Control Panel", page_icon="🎛️", layout="wide"
    )

    st.title("🎛️ Analytics Platform Control Panel")

    # Sidebar
    with st.sidebar:
        st.header("⚡ Commands")

        st.subheader("🐳 Docker")
        if st.button("🔥 FULL STARTUP", use_container_width=True, type="primary"):
            st.session_state.docker_logs.clear()
            st.session_state.streaming_logs.clear()
            st.session_state.simulator_logs.clear()
            # 1. Start Infra
            _run_in_thread(
                ["docker", "compose", "up", "-d"],
                st.session_state.docker_logs,
                "Full Start (Infra)",
            )
            # 2. Start Streaming (it has depends_on healthy but let's be explicit)
            _run_in_thread(
                ["docker", "compose", "up", "-d", "streaming"],
                st.session_state.streaming_logs,
                "Full Start (Streaming)",
            )
            # 3. Run Simulator
            _run_in_thread(
                ["docker", "compose", "run", "--rm", "simulator", "--mode", "both"],
                st.session_state.simulator_logs,
                "Full Start (Simulator)",
            )

        if st.button("▶️ Start Infra", use_container_width=True):
            st.session_state.docker_logs.clear()
            _run_in_thread(
                ["docker", "compose", "up", "-d"],
                st.session_state.docker_logs,
                "Infra Start",
            )

        if st.button("⏹️ Stop All", use_container_width=True):
            _run_in_thread(
                ["docker", "compose", "down"],
                st.session_state.docker_logs,
                "Infra Stop",
            )

        st.divider()

        st.subheader("🎰 Simulator")
        sim_mode = st.selectbox("Mode", ["simulate", "seed", "both"], index=0)
        if st.button("🚀 Run Simulator", use_container_width=True):
            st.session_state.simulator_logs.clear()
            _run_in_thread(
                ["docker", "compose", "run", "--rm", "simulator", "--mode", sim_mode],
                st.session_state.simulator_logs,
                f"Simulator ({sim_mode})",
            )

        st.divider()

        st.subheader("⚡ Streaming")
        if st.button("🔌 Start Spark", use_container_width=True):
            st.session_state.streaming_logs.clear()
            _run_in_thread(
                ["docker", "compose", "up", "-d", "streaming"],
                st.session_state.streaming_logs,
                "Streaming",
            )

        st.divider()
        auto_refresh = st.toggle("Auto-refresh (5s)", value=True)

    # Main Area
    col_status, col_logs = st.columns([1, 2])

    with col_status:
        st.subheader("📊 System Status")
        _render_status_table(_get_container_status())

        st.divider()
        st.markdown(
            """
        ### 🔗 Access UIs
        - 🌐 [Airflow](http://localhost:8088)
        - 📊 [AKHQ (Kafka)](http://localhost:8080)
        - 📈 [Grafana](http://localhost:3001)
        """
        )

    with col_logs:
        st.subheader("📺 Terminal Output")
        tab1, tab2, tab3 = st.tabs(["Docker", "Simulator", "Streaming"])
        with tab1:
            _render_log_panel(st.session_state.docker_logs, "Docker Logs")
        with tab2:
            _render_log_panel(st.session_state.simulator_logs, "Simulator Logs")
        with tab3:
            _render_log_panel(st.session_state.streaming_logs, "Streaming Logs")

    if auto_refresh:
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
