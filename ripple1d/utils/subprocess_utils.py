import win32api
import win32con


def send_ctrl_c_event(pid: int) -> None:
    """Send the Ctrl+C event to a specific pid."""
    try:
        win32api.GenerateConsoleCtrlEvent(win32con.CTRL_C_EVENT, pid)
    except SystemError as e:
        # When the subprocess was initialized in a different process group, you may not send signals to it.
        # If that's the case this func will raise "SystemError: Exception occurred: (87, 'GenerateConsoleCtrlEvent', 'The parameter is incorrect.')"
        raise RuntimeError(f"Attempted to send Ctrl+C to PID {pid}, but it was created in a different process group")
    return {"sent_keyboard_interrupt": True}
