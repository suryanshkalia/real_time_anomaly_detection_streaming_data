import asyncio
import time
from nodes import Node
from constants import STOP

class Graph:
    def __init__(self):
        self.nodes = {}
        self.stop_event = asyncio.Event()
        # this is a dead letter queue that stores the failed node after rertry fails
        self.dlq = asyncio.Queue()

        self.forward = {}   # forward data flow
        self.backward = {}  # reverse dependencies
        #controls for dynamic producer handling
        self.target_pressure = 0.4
        self.max_sleep = 0.5
        self.min_sleep = 0.0
        self.current_sleep = 0.01 # starting point
        self.adjust_rate = 0.05  #rate of reacting
        self.MAX_LATENCY = 1.0 # seconds

        #for watermarking
        self.max_event_ts = float("-inf")
        self.allowed_late = 1.0 #events may arrive 1 second late
        self.current_watermark = float("-inf") #stream progress marker


    # this will store the pressure for each queue and max pressure will be returned
    def get_global_pressure(self):
        pressures = []

        for node in self.nodes.values():
            if node.inputs: # only consumer queues
                q = node.input_queue
                if q.maxsize > 0:
                    pressures.append(q.qsize()/ q.maxsize)
        return max(pressures) if pressures else 0
    # max will give the slowest, most congested queue so the producer can react accordingly even if the other queue's are not that filled up
    # so if one queue has 0.99 pressure then global is this 0.99

    def add_node(self, node: Node):
        self.nodes[node.id] = node

        self.forward[node.id] = set()
        self.backward[node.id] = set()

    def add_edge(self, from_id, to_id):
        if from_id not in self.nodes or to_id not in self.nodes:
            raise ValueError(f"INvalid edge {from_id}->{to_id}")

        self.forward[from_id].add(to_id)
        self.backward[to_id].add(from_id)

        self.nodes[from_id].outputs.add(to_id)
        self.nodes[to_id].inputs.add(from_id)

    def build(self, node_part, graph_part):
        for node_id, meta in node_part.items():
            node = Node(node_id, meta['coro'])
            self.add_node(node)

        for src, target in graph_part.items():
            if target:
                for tgt in target:
                    self.add_edge(src, tgt)

    async def run_node(self, node: Node):

        try:
            if not node.inputs: # producer node
                try:
                    await node.coro(
                        grph = self, # the graph object
                        output_queue = self._fanout_queue(node), # parallel push downstream to multiple nodes
                        stop_event = self.stop_event  # stop the producer
                        )
                except Exception as e:
                    print(f"Producer {node.id} crashed : {e}")
            else:
                while True: # worker node

                    priority, data = await node.input_queue.get()

                    if data is STOP:
                        print(f"{node.id} recieved STOP")

                        await asyncio.gather(
                            *[
                                self.nodes[out_id].input_queue.put(
                                    (float('inf'), STOP)
                                    )
                                for out_id in node.outputs
                                for i in range(self.nodes[out_id].workers)
                                ]
                            )

                        node.input_queue.task_done()
                        break

                    if self.is_late(data):
                        print(f"Late Event Dropped: {data['id']}")
                        node.input_queue.task_done()
                        continue

                    if self.get_global_pressure() > 0.7:
                        await asyncio.sleep(0.01)

                    try:
                        for attempt in range(node.retries): # 2 retries
                            try:
                                if node.outputs:
                                    result = await node.coro(
                                        grph = self,
                                        node = node,
                                        input_data = data
                                        )

                                    if result is STOP:

                                        print(f"{node.id} forwarding STOP")

                                        await asyncio.gather(*[
                                            self.nodes[out_id].input_queue.put(
                                                (float('inf'), STOP)
                                                )
                                            for out_id in node.outputs
                                            for i in range(self.nodes[out_id].workers)
                                            ])

                                        break

                                    deadline = result["ts"] + self.MAX_LATENCY
                                    priority = deadline

                                    await asyncio.gather(*[
                                        self.nodes[out_id].input_queue.put(
                                            (priority, result)
                                            )
                                        for out_id in node.outputs
                                        ])

                                else:
                                    await node.coro(
                                        grph = self,
                                        node = node,
                                        input_data = data
                                        ) # sink

                                node.processed += 1 # after first is processed we cal curr time

                                curr_time = asyncio.get_event_loop().time()

                                if node.first_item_time is None:
                                    node.first_item_time = curr_time # first item processsrd at curr time

                                node.last_item_time = curr_time # end time
                                # done to get the only time when workers are engaged

                                break

                            except Exception as e:
                                if attempt == node.retries - 1:
                                    # insert the failed node in the dead letter queue for later introsp.
                                    await self.dlq.put((node.id, data))
                                    print(f"{node.id} failed permanently: ", e)
                                    node.failed += 1
                                else:
                                    await asyncio.sleep(0.2) # 0.2 for retry delay

                    finally:
                        node.input_queue.task_done()

        except asyncio.CancelledError:
            print(f"{node.id} cancelled")
            raise

    def _fanout_queue(self, node):
        class Fanout:
            def __init__(self, graph, node):
                self.graph = graph
                self.node = node

            async def put(self, item):
                priority, data = item

                if data is STOP:
                    for out_id in self.node.outputs:
                        await self.graph.nodes[out_id].input_queue.put(
                            (float('inf'), STOP)
                            )
                    return

                self.graph.update_watermark(data["ts"])

                #drop stale before enqueue
                if time.time() - data["ts"] > self.graph.MAX_LATENCY:
                    print("dropping before enqueue")
                    return

                pressures= []

                #checking downstream presssure befrore pushing
                for out_id in self.node.outputs:
                    q = self.graph.nodes[out_id].input_queue
                    if q.maxsize > 0:
                        pressures.append(q.qsize() / q.maxsize)

                max_pressure = max(pressures) if pressures else 0

                if max_pressure > 0.8:
                    await asyncio.sleep(0.05)

                #pushing now
                for out_id in self.node.outputs:
                    q = self.graph.nodes[out_id].input_queue
                    await q.put((priority, data))

                    print(
                        f"push-> {out_id} | "
                        f"size = {q.qsize()}"
                        f"pressure = {q.qsize()/q.maxsize:.2f}"
                        )
        return Fanout(self, node)

    #producer gets fake queue, which pushes internally into multiple queues

    async def dlq_handler(self):
        while not self.stop_event.is_set() or not self.dlq.empty():
            try:
                item = await self.dlq.get()   # recieve the failed nodes/messages
                print("DLQ nodes: ", item)
                self.dlq.task_done()
            except asyncio.CancelledError:
                print("DLQ handler cancelled")
                raise

    async def start(self):
        tasks = []

        for node in self.nodes.values():
            worker_count = node.workers if node.inputs else 1 # only 1 producer

            for i in range(worker_count):   # there are 3 workers for each node but should not be for producer
                tasks.append(asyncio.create_task(self.run_node(node)))

        tasks.append(asyncio.create_task(self.monitor()))
        tasks.append(asyncio.create_task(self.dlq_handler()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("graph shutdown")

        finally:
            self.stop_event.set()

            for i in tasks:
                if not i.done():
                    i.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        print("\n----Final system metrics----")

        for node in self.nodes.values():
            #skipping producer
            if not node.inputs: # no input->producer
                print(f"{node.id}: producer node")
                continue

            if node.first_item_time and node.last_item_time:

                time_elapsed = (
                    node.last_item_time -
                    node.first_item_time
                    )

                throughput = (
                    node.processed / time_elapsed
                    if time_elapsed > 0 else 0
                    )
            else:
                throughput = 0

                print(f"{node.id}: {throughput: .2f} items/sec")

        # system throughput
        sinks = [ n for n in self.nodes.values() if not n.outputs ]

        times = [
            (n.last_item_time - n.first_item_time)
            for n in sinks
            if n.first_item_time and n.last_item_time
            ]

        if times:
            time_elapsed = max(times)
            total_processed = sum(n.processed for n in sinks)

            system_throughput = total_processed/time_elapsed
            print(f"system throughput: {system_throughput: .2f} items/sec")

    # live monitor
    async def monitor(self):
        while not self.stop_event.is_set():
            for node in self.nodes.values():
                q = node.input_queue

                pressure = (q.qsize() / q.maxsize ) if q.maxsize > 0 else 0

                print(
                    f"{node.id} | "
                    f" processed = {node.processed} "
                    f" queue = {q.qsize()}"
                    f" pressure = {pressure:.2f}"
                    f" watermark = {self.current_watermark:.3f} "
                    )

            print(f"Global Pressure: {self.get_global_pressure():.2f}\n")
            await asyncio.sleep(1)

    def calculate_sleep(self):
        pressure = self.get_global_pressure()

        error = pressure - self.target_pressure

        self.current_sleep += error * self.adjust_rate
        self.current_sleep = max(self.min_sleep, min(self.max_sleep, self.current_sleep))

        return self.current_sleep, pressure

    # water mark will update during ingestion, meaing in run_node or fanout
    def update_watermark(self, event_ts):
        self.max_event_ts = max(
            self.max_event_ts,
            event_ts
            )

        self.current_watermark = (
            self.max_event_ts - self.allowed_late
            )

    # is the event late?
    def is_late(self, event):
        return event["ts"] < self.current_watermark

