import subprocess
import time


def restart_indexer():
    while True:
        print("🚀 Starting Bitcoin Indexer...")
        process = subprocess.Popen(["make", "run-indexer"])
        process.wait()  # Wait for the process to exit

        print("❌ Bitcoin Indexer crashed! Restarting in 10 seconds...")
        time.sleep(10)


if __name__ == "__main__":
    restart_indexer()
