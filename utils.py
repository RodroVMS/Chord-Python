def get_source_ip():
    import subprocess

    # address = subprocess.check_output(["hostname", "-s", "-I"]).decode("utf-8")[:-2]
    address = subprocess.check_output(["hostname", "-i"]).decode("utf-8")[:-1]
    return address

def zpipe(ctx):
    import zmq
    import binascii
    import os

    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


import socket
BEACON_PORT = 8001
def net_beacon(ip):
    # print("Beacon Online ...")
    beacon_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    beacon_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    beacon_socket.bind(("", BEACON_PORT))
    while True:
        info, addr = beacon_socket.recvfrom(1024)
        if addr[0] == ip:
            continue
        if info == b"PING":
            # print(f"Deteced signaling peer: {addr[0]}:{addr[1] - BEACON_SOCK}")
            beacon_socket.sendto("PONG".encode(), addr)

def find_nodes() -> str:
    print("Searching for On-line Nodes ...")
    broadcast_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(0.5)
    tolerance = 3
    while tolerance > 0:
        broadcast_socket.sendto(
            "PING".encode("utf-8"), ("<broadcast>", BEACON_PORT)
        )
        try:
            info, addr = broadcast_socket.recvfrom(1024)
            if info == b"PONG":
                print(f"Found online peer: {addr[0]}:{BEACON_PORT - 1}")
                broadcast_socket.close()
                return addr[0]
        except socket.timeout:
            tolerance -= 1
    broadcast_socket.close()
    return ""


import zmq.sugar as zmq
import time
def recieve_multipart_timeout(sock, timeout):
    start = time.time()
    while time.time() -  start < timeout:
        try:
            res = sock.recv_multipart(zmq.NOBLOCK)
            return res
        except zmq.error.Again:
            continue
    return []