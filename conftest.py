
import platform

collect_ignore = ["setup.py"]

if platform.system() != "Windows":
    collect_ignore.append("plaidcloud/rpc/pcm_connection.py")
