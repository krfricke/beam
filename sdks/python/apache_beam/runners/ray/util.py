import socket
import sys


def get_my_ip():
    if sys.platform == "darwin":
        return "localhost"
    else:
        return socket.gethostname()[3:].replace("-", ".")
