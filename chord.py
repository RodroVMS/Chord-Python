import logging
import time 

import zmq.sugar as zmq
from zmq.sugar import poll

from chord_const import *
from utils import get_source_ip, zpipe, net_beacon, find_nodes, recieve_multipart_timeout
from sortedcontainers.sortedset import SortedSet
from threading import Thread, Lock

# TODo: Eliminar del diccionario send_ids de Request Handler

class ChordNode:
    def __init__(self, m) -> None:
        '''
        Start a chord node.
        ### Parameters:
        m: Represents the bits of the identifier.
        '''
        self.ip = get_source_ip()
        self.port = REP_PORT
        self.online = False
        self.joined = False

        self.bits = m
        self.MAX_CONN = 2**m
        self.node_id = hash(self.ip) % self.MAX_CONN

        self.finger_table = [None for _ in range(m + 1)] # ft[0] = predecessor
        self.node_set = SortedSet([(self.node_id, self.ip)])

        self.ctx = zmq.Context()
        self.ip_router_table = dict() # To know a socket router id knowing the host's IP
        self.usr_pipe = zpipe(self.ctx) # Pipe to connect user with request_recv thread
        self.rr_pipe = zpipe(self.ctx) # Pipe for communication between request sender and request reciver threads
        
        # Stabilize
        self.ancestor_last_seen = 0

        # Sync
        self.ip_table_lock = Lock()
        self.set_lock = Lock()

        # Debbuging and Info
        self.logger.setLevel(logging.INFO)
        self.logger = logging.Logger("chord log")
    
    def lookup(self, key:int) -> str:
        '''
        Yields the IP address of the node responsible
        for the key.
        '''
        if not self.online:
            return "Node is off-line."
        if not self.joined:
            return "Node is alone in the net :("

        self.usr_pipe[0].send_multipart([ASK_SUCC, int.to_bytes(key, self.bits, 'big')])
        holder = self.usr_pipe[0].recv_multipart()
        return holder
    
    def find_succesor(self, key:int):
        '''
        Finds the inmediate predecessor node of the
        identifier; the successor of that node is the
        successor of that identifier.
        '''
        if not self.online:
            return "Node is off-line."
        if not self.joined:
            return "Node is alone in the net :("
        
        self.usr_pipe[0].send_multipart([ASK_SUCC, int.to_bytes(key, self.bits, 'big')])
        succ_id = self.usr_pipe[0].recv_multipart()
        return succ_id

    def find_predecessor(self, key:int):
        '''
        Returns the predecessor of the key.
        '''
        if not self.online:
            return "Node is off-line."
        if not self.joined:
            return "Node is alone in the net :("
        
        self.usr_pipe[0].send_multipart([ASK_PRED, int.to_bytes(key, self.bits, 'big')])
        pred_id = self.usr_pipe[0].recv_multipart()
        return pred_id
    
    def join(self, ip=""):
        '''
        Joins the node to a Chord network. Initialices
        predecessors and finger table asking the joined
        node. Notify the higher layer software the keys
        this node is responsible for.
        '''
        if not self.online:
            return "Node is off-line."
       
        if self.joined:
            return "Already in the net with some friends."
        
        if ip == self.ip or ip == "":
            ip = find_nodes()
            if not ip or ip == self.ip:
                return 'Could not found online nodes'

        self.usr_pipe[0].send_multipart([ASK_JOIN, ip.encode()])
        answer = self.usr_pipe[0].recv_multipart()
        return answer
        
    def exit(self):
        '''
        Stop the node. Close sockets and exit.
        '''
        self.usr_pipe[0].send_multipart([STOP, b''])
        answer = self.usr_pipe[0].recv_multipart()
        self.online = False
        return answer

    def run(self):
        '''
        Starts the chord node.
        '''
        self.online = True
        t1 = Thread(target=self.__reply_loop)
        t2 = Thread(target=self.__request_loop)
        t3 = Thread(target=net_beacon, args=(self.ip, ), daemon=True)

        t1.start()
        t2.start()
        t3.start()
        print(f"Node({self.node_id}) running on {self.ip}:{REP_PORT}")

    def __request_loop(self):
        send_router = self.ctx.socket(zmq.ROUTER)
        send_router.probe_router = 1

        poller = zmq.Poller()
        poller.register(self.usr_pipe[1], zmq.POLLIN)
        poller.register(self.rr_pipe[1], zmq.POLLIN)

        self.__get_id_from_ip_table(send_router, self.ip, self.ip_router_table)

        while self.online:
            sock_dict = dict(poller.poll(TIMEOUT_STABILIZE))
            if self.usr_pipe[1] in sock_dict:
                request = self.usr_pipe[1].recv_multipart(zmq.NOBLOCK)
                answer = self.__request_handler(request, send_router)
                if answer is not None:
                    self.usr_pipe[1].send_multipart([answer])
            elif self.rr_pipe[1] in sock_dict:
                request = self.rr_pipe[1].recv_multipart(zmq.NOBLOCK)
                answer = self.__request_handler(request, send_router)
                if answer is not None:
                    self.rr_pipe[1].send_multipart([answer])
            elif self.joined:
                self.__request_stabilize(send_router, self.ip_router_table)
        
        send_router.close()

    def __reply_loop(self):
        '''
        Node starts a routine where it stabilizes periodically.
        It also handles request from other nodes.
        '''
        
        recv_router = self.ctx.socket(zmq.ROUTER)
        recv_router.bind(f"tcp://{self.ip}:{REP_PORT}")

        while self.online:
            try:
                request = recv_router.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.error.Again:
                time.sleep(0.1)
                continue

            if len(request) == 2:
                idx, _ = request
                print("Sending ack", request)
                recv_router.send_multipart([idx, ACK, int.to_bytes(self.node_id, 1, 'big')])
                continue

            self.__reply_handler(request, recv_router)
        recv_router.close()

    def __request_handler(self, request, send_router:zmq.Socket):
        flag, extra = request
        print("Sending request", flag)

        if flag == ASK_JOIN:
            return self.__request_join(extra, send_router, self.ip_router_table)
        if flag == ASK_SUCC or flag == ASK_PRED:
            return self.__request_predsuccessor(flag, extra, send_router, self.ip_router_table)

        if flag == STOP:
            return self.__notify_leave(send_router, self.ip_router_table)
    
    def __reply_handler(self, request, recv_router:zmq.Socket):
        idx, sender_ip, flag, extra = request
        print("Replying to", flag,"from",sender_ip)

        if flag == ASK_JOIN:
            return self.__reply_join(idx, sender_ip, extra, recv_router)
        if flag == ASK_SUCC or flag == ASK_PRED:
            return self.__reply_predsuccesor(idx, flag, extra, recv_router)
        if flag == ASK_STAB:
            return self.__reply_stabilize(idx, sender_ip, extra, recv_router)
        
        if flag == LEAVE:
            self.__acknowledge_leave(sender_ip, extra)
    
    def __request_stabilize(self, send_router:zmq.Socket, send_ids):
        '''
        Ask succesor for predecessor. If this node is not the
        closest predecessor, ask successor's predecessor for
        predecessor and so on, until one is found. This node
        succesor is the one whose predecessor is this.
        '''
        succ_id, succ_ip = self.finger_table[1]; print(f">Starting stabilize. Current succ {succ_id}:{succ_ip}")
        node_set = self.node_set.copy()
        node_set.remove((succ_id, succ_ip))

        known_ips = [(succ_id, succ_ip)] + [node for node in node_set]
        new_ips = []
        known_index = -1
        new_index = -1
        while known_index < len(known_ips) - 1:
            if new_index < len(new_ips) - 1:
                new_index += 1
                node_id, node_ip = new_ips[new_index]
            else:
                known_index += 1
                node_id, node_ip = known_ips[known_index]
            
            other_router_id = self.__get_id_from_ip_table(send_router, node_ip, send_ids)
            send_router.send_multipart(
                [other_router_id, self.ip.encode(), ASK_STAB, int.to_bytes(self.node_id, self.bits, 'big')]
            )

            reply = recieve_multipart_timeout(send_router, 8)
            if len(reply) == 0:
                self.logger.warning(f"Could not stabilize. Could not connect to {node_ip}:{REP_PORT}. Trying with other known node.")
                self.__remove_node(node_id, node_ip)
                self.__remove_id_from_ip_table(node_ip)
                self.__update_finger_table()
                continue
            _, flag, info = reply
            if flag != ANS_STAB:
                raise Exception(f"Recieved bad flag. Was expecting {ANS_STAB} but was recieved {flag}")
            pred_id, pred_ip = info.split(b':')
            pred_id = int.from_bytes(pred_id, 'big')
            pred_ip = pred_ip.decode()
            
            if self.node_id == pred_id:
                print(f"Ancestor of {node_id}:{node_ip} is still me",)
                break
            
            print(f"!!Ancestor of {node_id}:{node_ip} changed to", pred_id, pred_ip)
            new_ips.append((pred_id, pred_ip))
        
        print("<Ending stabilize")
        self.__update_finger_table()
    
    def __reply_stabilize(self, router_id, stab_ip, extra, recv_router:zmq.Socket):
        '''
        Reply to stabilize requests from other nodes.
        '''
        pos_pred_id = int.from_bytes(extra, 'big')
        pred_id, pred_ip = self.finger_table[0]

        self.__add_node(pos_pred_id, stab_ip.decode())

        if self.__in_between(pos_pred_id, pred_id + 1, self.node_id) or time.time() - self.ancestor_last_seen  <= 0:
            self.ancestor_last_seen = time.time() + (TIMEOUT_STABILIZE*3)/1000
            recv_router.send_multipart([router_id, ANS_STAB, extra + b':' + stab_ip])
            self.__update_finger_table()
        else:
            recv_router.send_multipart([router_id, ANS_STAB, int.to_bytes(pred_id, self.bits, 'big') + b':' + pred_ip.encode()])

    def __notify_leave(self, send_router, send_ids):
        '''
        Notify known nodes that this node is leaving the net to never comeback. 
        '''
        for node_ip in send_ids:
            node_router_id = send_ids[node_ip]
            send_router.send_multipart[node_router_id, self.ip.encode(), LEAVE, int.to_bytes(self.node_id, self.bits, 'big')]
        return b'Done'

    def __acknowledge_leave(self, node_ip, node_id):
        '''
        Acknowledge a node is leaving the net to never comeback. 
        '''
        node_id = int.from_bytes(node_id, 'big')
        node_ip = node_ip.decode()
        self.__remove_node(node_id, node_ip)
        self.__remove_id_from_ip_table(node_ip)
        self.__update_finger_table()

        try:
            del self.ip_router_table[node_ip]
        except KeyError:
            pass

    def __request_predsuccessor(self, flag, extra, send_router, send_ids):
        '''
        Request for predecessor or successor of a given key
        '''
        key = int.from_bytes(extra, 'big')
        pred = True if flag == ASK_PRED else False

        node_id, node_ip = self.__get_node_with_key(key, pred)
        other_router_id = self.__get_id_from_ip_table(send_router, node_ip, send_ids)
        extra = int.to_bytes(self.node_id, self.bits, 'big') + b':' + extra 
        print(f"Node {node_id} responsible for key {key}")
        
        if node_id == self.node_id:
            return int.to_bytes(self.node_id, self.bits, 'big') + b':' +  self.ip.encode()
        
        if node_id == key:
            return int.to_bytes(node_id, self.bits, 'big') + b':' + node_ip.encode()
        
        send_router.send_multipart(
        
            [other_router_id, self.ip.encode(), flag, extra]
        )
        reply = send_router.recv_multipart()
        _, _, info = reply
        print("Recieved info")
        return info

    def __reply_predsuccesor(self, router_id, flag, extra, recv_router:zmq.Socket):
        '''
        Reply for predecessor or successor of a given key
        '''
        node_id, key = extra.split(b':') 
        node_id = int.from_bytes(node_id, 'big')
        key = int.from_bytes(key, 'big')
        
        pred = True if flag == ASK_PRED else False
        ans_flag = ANS_PRED if flag == ASK_PRED else ANS_SUCC

        next_id, next_ip = self.__get_node_with_key(key, pred)
        if next_id == self.node_id:
            info = int.to_bytes(self.node_id, self.bits, 'big') + b':' +  self.ip.encode()
            recv_router.send_multipart([router_id, ans_flag, info])
            return
        
        if next_id == node_id:
            info = int.to_bytes(node_id, self.bits, 'big') + b':' +  next_ip.encode()
            recv_router.send_multipart([router_id, ans_flag, info])
            return
        
        self.rr_pipe[0].send_multipart([flag, extra])
        info = self.rr_pipe[0].recv_multipart()[0]
        recv_router.send_multipart([router_id, ans_flag, info])

    def __request_join(self, extra, send_router, send_ids):
        '''
        Request for a known node in the chord node for succesor and ancestor.
        '''
        print("Requesting join")
        ip = extra.decode()
        other_router_id = self.__get_id_from_ip_table(send_router, ip, send_ids)
        print("Obtaining other router id", other_router_id)
        if other_router_id == b'':
            self.logger.warning(f"Could not join to {ip}")
            return b''
        
        print("Sending join1")
        send_router.send_multipart(
            [
                other_router_id,
                self.ip.encode(),
                ASK_JOIN,
                int.to_bytes(self.node_id, self.bits, 'big')
            ]
        )
        print("Sending join2")
        reply = recieve_multipart_timeout(send_router, 8)
        print("Recieving reply", reply)
        if len(reply) == 0:
            self.logger.warning(f"Could not join to {ip}")
            return b''
        _, flag, extra = reply
        assert flag == ANS_JOIN, "Wrong flag"
        print("Extra before", extra)
        extra = extra.split(b'@')
        print("extra recieved\n", extra)
        pred_id, pred_ip = extra[0].split(b':')
        succ_id, succ_ip = extra[1].split(b':')

        print("recieveing in join", pred_id, succ_id)
        pred_id = int.from_bytes(pred_id, 'big')
        succ_id = int.from_bytes(succ_id, 'big')
        
        pred_ip = pred_ip.decode()
        succ_ip = succ_ip.decode()
        self.__add_node(pred_id, pred_ip)
        self.__add_node(succ_id, succ_ip)
        self.__update_finger_table()

        self.joined = True
        return b'Something here'
    
    def __reply_join(self, router_id, sender_ip, extra, recv_router):
        '''
        Finds succesor and ancestor of joining node.
        '''
        sender_ip = sender_ip.decode()
        join_node_id = int.from_bytes(extra, 'big')
        self.__add_node(join_node_id, sender_ip)
        self.__update_finger_table()

        self.rr_pipe[0].send_multipart([ASK_PRED, extra])
        info_pred = self.rr_pipe[0].recv_multipart()[0]

        self.rr_pipe[0].send_multipart([ASK_SUCC, extra])
        info_succ = self.rr_pipe[0].recv_multipart()[0]

        print("Sendinf in join",info_pred, info_succ)
        info = info_pred + b'@' + info_succ
        
        self.joined = True
        recv_router.send_multipart([router_id, ANS_JOIN, info])


    def __get_id_from_ip_table(self, router, ip, ip_table) -> bytes:
        '''
        Get the router id of a chord node according to its IP.
        '''
        try:
            return ip_table[ip]
        except KeyError:
            print("Connecting to", ip)
            router.connect(f"tcp://{ip}:{REP_PORT}")
            try:
                router_id, flag, node_id = recieve_multipart_timeout(router, 8)
            except ValueError:
                return b''
            if flag != ACK:
                self.logger.warning(f"SEND: Recieved {flag} while waiting for ack from {ip}")
                return b''
            print("Recieved ACK", router_id, "||End||")
            ip_table[ip] = router_id
            node_id = int.from_bytes(node_id, 'big')
            self.__add_node(node_id, ip)
            self.__update_finger_table()
        
        return ip_table[ip]

    def __remove_id_from_ip_table(self, ip):
        self.ip_table_lock.acquire()
        try:
            del self.ip_router_table[ip]
        except KeyError:
            pass
        self.ip_table_lock.release()
    
    def __update_finger_table(self):
        '''
        Recomputes finger table.
        '''
        self.set_lock.acquire()

        node = (self.node_id, self.ip)
        self.finger_table[0] =  self.node_set[self.node_set.index(node) -1]

        for i in range(1, self.bits + 1):
            succ = (self.node_id + 2 **(i-1)) % self.MAX_CONN
            lower_bound = self.node_set.index(node)
            upper_bound = (lower_bound + 1) % len(self.node_set)
            for _ in range(len(self.node_set)):
                if self.__in_between(
                    succ, self.node_set[lower_bound][0] + 1,
                    self.node_set[upper_bound][0] +1
                    ):
                    self.finger_table[i] = self.node_set[upper_bound]
                    break
                lower_bound = upper_bound
                upper_bound = (upper_bound + 1) % len(self.node_set)
            else:
                self.finger_table[i] = None

        self.set_lock.release()

    def __get_node_with_key(self, key:int, predecessor=False):
        '''
        Returns the predecessor or the successor of the key.
        '''
        if self.__in_between(key, self.finger_table[0][0] + 1, self.node_id + 1):
            return (self.node_id, self.ip) if not predecessor else self.finger_table[0]
        if self.__in_between(key, self.node_id + 1, self.finger_table[1][0] + 1):
            return self.finger_table[1] if not predecessor else (self.node_id, self.ip)
        for i in range(1, self.bits + 1):
            if self.__in_between(key, self.finger_table[i][0] + 1, self.finger_table[(i + 1)% self.bits][0] + 1):
                return self.finger_table[(i + 1) %self.bits] if not predecessor else (self.node_id, self.ip)
        print(self.finger_table)
        raise Exception(f"Node must be always found. Key: {key}")

    def __in_between(self, key:int, lower_bound, upper_bound) -> bool:
        '''
        Returns true if key is in the specified bounds. Inclusive
        lower bound. Exclusive upper bound.
        '''
        if lower_bound <= upper_bound:
            return lower_bound <= key < upper_bound
        
        return (
            (lower_bound <= key < upper_bound + self.MAX_CONN) or
            (lower_bound <= key + self.MAX_CONN and key < upper_bound)
        )

    def __add_node(self, node_id, node_ip):
        '''
        Add a new node to known online nodes by this node.
        '''
        self.set_lock.acquire()
        self.node_set.add((node_id, node_ip))
        self.set_lock.release()

    def __remove_node(self, node_id, node_ip):
        '''
        Forgets a known node
        '''
        self.set_lock.acquire()
        try:
            self.node_set.remove((node_id, node_ip))
        except KeyError:
            pass
        if len(self.node_set) == 1:
            self.online = False
        self.set_lock.release()
  