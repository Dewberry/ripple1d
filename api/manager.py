"""Manage Flask and Huey."""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime

import psutil


class RippleManager:
    """Launch Flask and Huey in separate terminals and manage their lifecycles."""

    def __init__(self, args):
        if args.command == "start":
            self.flask = {
                "flask_debug": args.flask_debug,
                "flask_port": args.flask_port,
                "flask_host": args.flask_host,
                "flask_shell": args.flask_shell,
            }
            self.huey = {
                "thread_count": args.thread_count,
                "huey_shell": args.huey_shell,
            }
            self.logs_dir = args.logs
            self.venv_path = args.venv_path
        self.pids_file = args.pids_file
        self.processes = []

    def print_config(self):
        """Pring configuration settings for the Ripple Manager."""
        print("Flask API Configuration:", self.flask)
        print("Huey Consumer Configuration:", self.huey)
        print("Logs Directory:", self.logs_dir)
        print("Processes File:", self.pids_file)
        print("Virtual Environment Path:", self.venv_path)

    def _handle_remove_readonly(self, func, path, exc_info):
        """Handle the read-only file deletion error."""
        import stat

        os.chmod(path, stat.S_IWRITE)
        func(path)

    def start(self):
        """Start the Ripple API and Huey consumer in separate terminals."""
        if sys.platform != "win32":
            raise SystemError("API can only be run from a windows system")

        huey_consumer_path = shutil.which("huey_consumer.py")

        if not huey_consumer_path:
            print("Error: huey consumer script was not discoverable.")
            exit(1)

        if os.path.exists(self.logs_dir):
            shutil.rmtree(self.logs_dir, onerror=self._handle_remove_readonly)

        if os.path.exists(self.logs_dir):
            print(f"Error: could not delete {self.logs_dir}")
            exit(1)

        os.makedirs(self.logs_dir, exist_ok=True)
        if not os.path.exists(self.logs_dir):
            print(f"Error: could not create {self.logs_dir}")
            exit(1)

        if self.venv_path:
            python_executable = os.path.join(self.venv_path, "Scripts", "python.exe")
        else:
            python_executable = sys.executable

        print("Starting ripple-huey")
        huey_command = [
            python_executable,
            "-u",
            huey_consumer_path,
            "api.tasks.huey",
            "-w",
            str(self.huey["thread_count"]),
        ]
        if self.huey["huey_shell"]:
            if sys.platform == "win32":
                huey_result = subprocess.Popen(["start", "cmd", "/k", " ".join(huey_command)], shell=True)
            else:
                huey_result = subprocess.Popen(["gnome-terminal", "--"] + huey_command)
        else:
            huey_result = subprocess.Popen(huey_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.processes.append((huey_result, "huey"))

        print("Starting ripple-flask")
        flask_command = [
            python_executable,
            "-m",
            "flask",
            "run",
            "--host",
            self.flask["flask_host"],
            "--port",
            str(self.flask["flask_port"]),
        ]

        if self.flask["flask_debug"]:
            print("Flask debug mode enabled")
            flask_command.append("--debug")

        if self.flask["flask_shell"]:
            flask_result = subprocess.Popen(["start", "cmd", "/k", " ".join(flask_command)], shell=True)
        else:
            flask_result = subprocess.Popen(flask_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Check if Flask started successfully
        try:
            flask_result.wait(timeout=5)
            if flask_result.returncode != 0:
                raise subprocess.CalledProcessError(flask_result.returncode, flask_command, stderr=flask_result.stderr)
            self.processes.append((flask_result, "flask"))
            print("Success!")
        except subprocess.TimeoutExpired:
            self.processes.append((flask_result, "flask"))
        except subprocess.CalledProcessError as e:
            error_message = e.stderr.read().decode() if e.stderr else "No error message captured."
            print(f"Error: Failed to start Flask. Error message: {error_message}")
            self.processes.append((None, "flask"))

        # Load existing PIDs from the file if it exists
        if os.path.exists(self.pids_file):
            with open(self.pids_file, "r") as f:
                existing_pids = json.load(f)
        else:
            existing_pids = []

        # Append new PIDs to the existing list with start time, status, and type
        new_pids = []
        for process, process_type in self.processes:
            pid = process.pid if process else None
            start_time = datetime.now().isoformat()
            stop_time = None
            if pid:
                status = "running"
            else:
                status = "failed"

            pid_info = {
                "pid": pid,
                "start_time": start_time,
                "stop_time": stop_time,
                "status": status,
                "type": process_type,
            }

            new_pids.append(pid_info)

        all_pids = existing_pids + new_pids

        with open(self.pids_file, "w") as f:
            json.dump(all_pids, f, indent=4)

    def stop(self):
        """Stop the Ripple API and Huey consumer."""
        try:
            with open(self.pids_file, "r") as f:
                pids_info = json.load(f)
        except FileNotFoundError:
            print(f"Error: PID file '{self.pids_file}' not found.")
            return

        for pid_info in pids_info:
            pid = pid_info["pid"]
            if pid is None:
                print(f"PID {pid} not found")
            try:
                process = psutil.Process(pid)
                process.terminate()
                process.wait(timeout=5)
                pid_info["status"] = "stopped"
                pid_info["stop_time"] = datetime.now().isoformat()
            except psutil.NoSuchProcess:
                error_message = f"Error terminating process PID: {pid}, error: process PID not found (pid={pid})"
                print(error_message)
                pid_info["error"] = error_message
                pid_info["status"] = "not found"
            except (psutil.AccessDenied, psutil.TimeoutExpired) as e:
                error_message = f"Error terminating process PID: {pid}, error: {e}"
                print(error_message)
                pid_info["error"] = error_message
                pid_info["status"] = "error"

        # Write the updated list of PIDs back to the file
        with open(self.pids_file, "w") as f:
            json.dump(pids_info, f, indent=4)

    def status(self):
        """Check the status of the Ripple API and Huey consumer."""
        try:
            with open(self.pids_file, "r") as f:
                pids_info = json.load(f)
        except FileNotFoundError:
            print(f"Error: PID file '{self.pids_file}' not found.")
            return

        for pid_info in pids_info:
            pid = pid_info["pid"]
            if pid is None:
                continue
            if psutil.pid_exists(pid):
                pid_info["status"] = "running"
            else:
                pid_info["status"] = "stopped"

        # Print the status of each process
        for pid_info in pids_info:
            print(
                f"PID: {pid_info['pid']}, Type: {pid_info['type']}, Start Time: {pid_info['start_time']}, Stop Time: {pid_info['stop_time']}, Status: {pid_info['status']}"
            )


def main():
    """Run Ripple in API mode."""
    parser = argparse.ArgumentParser(description="Ripple Manager Configuration")
    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # Define the 'start' subcommand
    start_parser = subparsers.add_parser("start", help="Start the Ripple Manager")
    start_parser.add_argument(
        "--flask_shell", action="store_true", help="Launch terminal for Flask API (default: False)"
    )

    start_parser.add_argument("--flask_debug", action="store_true", help="Debug mode for Flask API (default: False)")
    start_parser.add_argument("--flask_port", type=int, default=5000, help="Port for Flask API (default: 5000)")
    start_parser.add_argument("--flask_host", type=str, default="0.0.0.0", help="Host for Flask API (default: 0.0.0.0)")
    start_parser.add_argument("--thread_count", type=int, default=1, help="Thread count for Huey Consumer (default: 1)")
    start_parser.add_argument(
        "--huey_shell", action="store_true", help="Launch terminal for Huey Consumer (default: False)"
    )
    start_parser.add_argument(
        "--logs",
        type=str,
        default=os.path.join(os.getcwd(), "logs"),
        help="Logs directory (default: current directory/logs)",
    )
    start_parser.add_argument(
        "--pids_file",
        type=str,
        default=os.path.join(os.getcwd(), "process-ids.json"),
        help="PIDs file (default: current directory/process-ids.json)",
    )
    start_parser.add_argument("--venv_path", type=str, default="", help="Virtual environment path (default: '')")

    # Define the 'stop' subcommand
    stop_parser = subparsers.add_parser("stop", help="Stop the Ripple Manager")
    stop_parser.add_argument(
        "--pids_file",
        type=str,
        default=os.path.join(os.getcwd(), "process-ids.json"),
        help="Process-IDs file (default: current directory/process-ids.json)",
    )

    # Define the 'status' subcommand
    status_parser = subparsers.add_parser("status", help="Check the status of the Ripple Manager")
    status_parser.add_argument(
        "--pids_file",
        type=str,
        default=os.path.join(os.getcwd(), "process-ids.json"),
        help="PIDs file (default: current directory/process-ids.json)",
    )

    args = parser.parse_args()

    if args.command == "start":
        manager = RippleManager(args)
        manager.print_config()
        manager.start()
    elif args.command == "stop":
        manager = RippleManager(args)
        manager.stop()
    elif args.command == "status":
        manager = RippleManager(args)
        manager.status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
