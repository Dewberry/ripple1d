import os
import shutil
import signal
import subprocess
import sys


def start():
    """Start the Ripple API and Huey consumer in separate terminals."""
    env_load_script = "api-load-env.bat"
    result = subprocess.run(["call", env_load_script], shell=True)
    if result.returncode != 0:
        exit(result.returncode)

    # Identify the full path to huey_consumer.py
    huey_consumer_path = subprocess.getoutput("where huey_consumer.py")
    if not huey_consumer_path:
        print("Error: huey consumer script was not discoverable.")
        input("Press enter to exit.")
        exit(1)

    # Set up logs directory
    logs_dir = "api/logs/"
    # print(f"Deleting logs dir if exists: {logs_dir}")
    if os.path.exists(logs_dir):
        shutil.rmtree(logs_dir)

    if os.path.exists(logs_dir):
        print(f"Error: could not delete {logs_dir}")
        input("Press enter to exit.")
        exit(1)

    # print(f"Creating logs dir: {logs_dir}")
    os.makedirs(logs_dir, exist_ok=True)
    if not os.path.exists(logs_dir):
        print(f"Error: could not create {logs_dir}")
        input("Press enter to exit.")
        exit(1)

    # Launch huey consumer
    print("Starting ripple-huey")
    huey_result = subprocess.Popen(
        [
            "cmd",
            "/k",
            f"{os.environ['VENV_PATH']}\\Scripts\\activate.bat && python -u {huey_consumer_path} api.tasks.huey -w {os.environ['HUEY_THREAD_COUNT']}",
        ],
        stdout=subprocess.DEVNULL,
        # TODO: update config to use CREATE_NO_WINDOW or CREATE_NEW_CONSOLE as an option
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    # Launch Flask app
    print("Starting ripple-flask")
    flask_result = subprocess.Popen(
        ["cmd", "/k", f"{os.environ['VENV_PATH']}\\Scripts\\activate.bat && flask run"],
        stdout=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    with open("pids.txt", "w") as f:
        f.write(f"{flask_result.pid}\n")
        f.write(f"{huey_result.pid}\n")


def stop():
    """Stop the Ripple API and Huey consumer."""
    with open("pids.txt", "r") as f:
        pids = [int(pid.strip()) for pid in f.readlines()]

    for pid in pids:
        print(pid)
        try:
            # r = os.kill(pid, signal.SIGTERM)
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], shell=True)
        except OSError as e:
            print(f"Error terminating process PID: {pid}, error: {e}")

    os.remove("pids.txt")


if __name__ == "__main__":
    sys.argv = sys.argv[1:]

    if len(sys.argv) == 0:
        print("Usage: ripple_api.py start|stop")
        sys.exit(1)
    elif sys.argv[0] == "start":
        print("Starting ripple.....")
        start()
    elif sys.argv[0] == "stop":
        print("Stopping ripple.....")
        stop()
    else:
        print("Usage: ripple_api.py start|stop")
        sys.exit(1)
