import socket, json, os, sys, time, threading
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from processing.dispatcher import Dispatcher

HOST = "127.0.0.1"
PORT = 5002

dispatcher = Dispatcher()


def handle(connection):
    connection.settimeout(10)
    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = connection.recv(4096)
            if not chunk:
                break
            buffer += chunk
        if not buffer:
            return
        request = json.loads(buffer.split(b"\n", 1)[0].decode())
        start = time.perf_counter()
        response = dispatcher.process_request(request)
        total_time = time.perf_counter() - start
        response["billing_node"] = "NODE-2"
        response["billing_processing_time"] = total_time
        response["billing_pid"] = os.getpid()
        connection.sendall((json.dumps(response) + "\n").encode())
    except Exception as error:
        try:
            connection.sendall(
                (json.dumps({"error": str(error), "node": "NODE-2"}) + "\n").encode()
            )
        except Exception:
            pass
    finally:
        connection.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(200)

    print(f"NODE-2 Billing Server running on {HOST}:{PORT}")
    print(f"PID: {os.getpid()}")
    print(f"Active rating nodes: {dispatcher.node_names}")

    # Was single-threaded (one connection processed at a time) - a hard
    # bottleneck no matter how much concurrency upstream sends. Each
    # connection now gets its own thread, same pattern as nodes 3-6.
    while True:
        connection, _ = server.accept()
        threading.Thread(target=handle, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
