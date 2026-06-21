import subprocess
import tempfile
import time
import os
import signal
import sys

def run_test():
    with tempfile.TemporaryDirectory(prefix="rosetest_") as test_dir:
        mount_dir = os.path.join(test_dir, "mount")
        meta_dir = os.path.join(test_dir, "meta")
        data_dir = os.path.join(test_dir, "data")
        os.makedirs(mount_dir)
        os.makedirs(meta_dir)
        os.makedirs(data_dir)

        rpc_port = 50051  # or dynamically find a free port

        # Build the binary first to avoid `go run` subprocesses escaping
        print("Building rose daemon...")
        subprocess.run(["go", "build", "-o", "rose_bin", "./cmd/rose/main.go"], check=True)

        print("Starting rose daemon...")
        proc = subprocess.Popen(
            ["./rose_bin",
             "--mount", mount_dir,
             "--metadir", meta_dir,
             "--datadirs", data_dir,
             "--rpc", f":{rpc_port}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(2) # Wait for mount to be ready
        if proc.poll() is not None:
            print("Daemon failed to start:", proc.stderr.read())
            sys.exit(1)

        test_file = os.path.join(mount_dir, "test.txt")
        test_data = b"Hello, Rose FUSE!"

        try:
            print(f"Creating file at {test_file}...")
            with open(test_file, "wb") as f:
                f.write(test_data)

            print(f"Reading file from {test_file}...")
            with open(test_file, "rb") as f:
                read_data = f.read()

            if read_data == test_data:
                print("Test passed! Read data matches written data.")
            else:
                print(f"Test failed! Expected {test_data}, got {read_data}")
                sys.exit(1)

        except Exception as e:
            print(f"Test failed with exception: {e}")
            sys.exit(1)

        finally:
            print("Shutting down daemon...")
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("Daemon did not shut down gracefully, killing...")
                proc.kill()

            print("Metadata DB dump:")
            out = subprocess.check_output(['sqlite3', meta_dir + '/rose.db', '.dump'])
            for line in out.decode('utf8').splitlines():
                if line.startswith('INSERT'):
                    print(line)


            print("Daemon logs (stdout):")
            print(proc.stdout.read())
            print("Daemon logs (stderr):")
            print(proc.stderr.read())

if __name__ == "__main__":
    run_test()
