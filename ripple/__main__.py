import os
import shutil
import subprocess
import sys


def load_env_variables(env_file=".env"):
    """Load env variables."""
    if not os.path.exists(env_file):
        print("Error: could not find path:", env_file)
        input("Press enter to exit.")
        sys.exit(1)
    with open(env_file) as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value


def check_required_variables():
    """Verify args included in .env."""
    required_vars = ["VENV_PATH", "HUEY_THREAD_COUNT"]
    for var in required_vars:
        if var not in os.environ:
            print(f"Error: variable {var} not defined.")
            input("Press enter to exit.")
            sys.exit(1)


def find_huey_consumer():
    """Search for the huey_consumer in the python evnironment."""
    result = subprocess.run(["where", "huey_consumer.py"], capture_output=True, text=True)
    if result.returncode != 0:
        print("Error: huey consumer script was not discoverable.")
        input("Press enter to exit.")
        sys.exit(1)
    return result.stdout.strip()


def setup_logs_dir(logs_dir="api/logs"):
    """Create a logs directory."""
    if os.path.exists(logs_dir):
        print(f"Deleting logs dir if exists: {logs_dir}")
        shutil.rmtree(logs_dir)
    print(f"Creating logs dir: {logs_dir}")
    os.makedirs(logs_dir, exist_ok=True)


def launch_huey_consumer(huey_consumer_path):
    """Launch an instance of huey."""
    print("Starting ripple-huey")
    command = f'start cmd.exe /k "{os.environ["VENV_PATH"]}\\Scripts\\activate.bat && python -u {huey_consumer_path} api.tasks.huey -w {os.environ["HUEY_THREAD_COUNT"]}"'
    subprocess.run(command, shell=True)


def launch_flask_app():
    """Launch Flask."""
    print("Starting ripple-flask")
    command = f'start cmd.exe /k "{os.environ["VENV_PATH"]}\\Scripts\\activate.bat && flask run"'
    subprocess.run(command, shell=True)


if __name__ == "__main__":
    load_env_variables()
    check_required_variables()
    huey_consumer_path = find_huey_consumer()
    setup_logs_dir()
    launch_huey_consumer(huey_consumer_path)
    launch_flask_app()
