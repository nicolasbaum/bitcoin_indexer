import signal
import subprocess
import sys
import time


def signal_handler(sig, frame):
    print("\n🛑 Shutting down Bitcoin Indexer...")
    sys.exit(0)


def restart_indexer():
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            print("🚀 Starting Bitcoin Indexer...")
            process = subprocess.Popen(["make", "run-indexer"])
            ret_code = process.wait()  # Wait for the process to exit

            print(f"Bitcoin Indexer exited with return code: {ret_code}")
            if ret_code == 0:
                print("✅ Bitcoin Indexer stopped gracefully. Exiting...")
                sys.exit(0)
            else:
                print("❌ Bitcoin Indexer crashed! Restarting in 10 seconds...")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down Bitcoin Indexer...")
            if process:
                process.terminate()
                process.wait()
            sys.exit(0)


if __name__ == "__main__":
    restart_indexer()
