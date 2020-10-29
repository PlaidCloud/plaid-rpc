
import platform

collect_ignore = ["setup.py"]

if platform.system() != "Windows":
    collect_ignore.append("plaidtools/pcm_connection.py")
