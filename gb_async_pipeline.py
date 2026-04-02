import asyncio
import uuid

# right now its batch style execution that i will turn into real-time flow engine
# now this is a dag streaming engine/ but not real-time like we need
# right now this is just high-thrp DAG executor not really what i need

class Node:
    def __init__(self, id, coro, workers=1, queue_size=100, retries=3):
        self.id = id
        self.coro = coro
        self.workers = workers
        self.retries = retries
        self.processed = 0
        self.failed = 0

        self.inputs = set()  #dependencies( b takes from a so b depends upon a)
        self.outputs = set()  #downstream nodes( where the data is flowing towards)

        self.input_queue = asyncio.Queue(maxsize=queue_size) # now if downstream is slow then producer will
        # try to be prodcue slowly, it's basic backpressure handling'

    @classmethod
    async def input_coro(cls, Processor=None, output_queue = None, stop_event = None):
        while not stop_event.is_set():
            data = str(uuid.uuid4())
            await output_queue.put(data)
            await asyncio.sleep(0)

    @classmethod
    async def reverse(cls, Processor=None, input_data=None):
        return input_data[::-1]

    @classmethod
    async def output_coro(cls, Processor=None, input_data=None):
        print("output: ", input_data)

class Graph:
    def __init__(self):
        self.nodes = {}
        self.stop_event = asyncio.Event()
        # this is a dead letter queue that stores the failed node after rertry fails
        self.dlq = asyncio.Queue()

        self.forward = {}   # forward data flow
        self.backward = {}  # reverse dependencies

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
            if not node.inputs:
                await node.coro(
                    output_queue = self._fanout_queue(node), # parallel push downstream to multiple nodes
                    stop_event = self.stop_event  # stop the producer
                    )
            else:
                while True: # worker node
                    if self.stop_event.is_set() and node.input_queue.empty():
                        break # break after draining the queues

                    try:
                        data = await asyncio.wait_for(node.input_queue.get(), timeout=1)
                    except asyncio.TimeoutError:
                        if self.stop_event.is_set():
                            break
                        continue

                    try:
                        for attempt in range(node.retries): # 2 retries
                            try:
                                if node.outputs:
                                    result = await asyncio.wait_for(node.coro(input_data = data),
                                    timeout=5) # process

                                    await asyncio.gather(*[
                                        self.nodes[out_id].input_queue.put(result)
                                        for out_id in node.outputs
                                        ]) # parallel push ot the downstream nodes

                                else:
                                    await node.coro(input_data = data) # sink

                                node.processed += 1
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

        class FanOut:
            def __init__(self, graph, node):
                self.graph = graph
                self.node = node

            async def put(self, data):
               await asyncio.gather(*[
                   self.graph.nodes[out_id].input_queue.put(data)
                   for out_id in self.node.outputs
                   ]) # why not fanout parallely?

        return FanOut(self, node)

    #producer gets fake queue, which pushes internally into multiple queues

    async def dlq_handler(self):
        while True:
            item = await self.dlq.get()   # recieve the failed nodes/messages
            print("DLQ nodes: ", item)

    async def start(self, run_time = 5):
        self.start_time = asyncio.get_event_loop().time() # to capture start time(thrp)

        tasks = []

        for node in self.nodes.values():
            worker_count = node.workers if node.inputs else 1 # only 1 producer

            for _ in range(worker_count):   # there are 3 workers for each node but should not be for producer
                tasks.append(asyncio.create_task(self.run_node(node)))

        tasks.append(asyncio.create_task(self.monitor()))
        tasks.append(asyncio.create_task(self.dlq_handler()))

        await asyncio.sleep(run_time)

        # triggering shutdown gracefuly
        self.stop_event.set()
        # part of the shutdown
        # wait for queues to drain
        await asyncio.gather(*[
            node.input_queue.join()  # waits till queue's are drained
            # actuall draining is done by the worker n odes( in run_node )
            # queue has internal counter which remains the same until the task is done comeplet
            # rhen counterf -1, join() waits till conuter is not 0( until all items are processed completely) ,
            # mat;ab jitne items put() hue utne hi taksdone() call hone chaihihye, never forget node.task_done() neto join() will never return and program will crash
            for node in self.nodes.values()
            ])

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        for node in self.nodes.values():
            node_throughput = node.processed / ( asyncio.get_event_loop().time() - self.start_time )
            print(f"{node.id}: {node_throughput} items/sec ")

        end_time = asyncio.get_event_loop().time()

        sinks = [ n for n in self.nodes.values() if not n.outputs ] # if not output = true then it is a sink node
        total_processed = sum(n.processed for n in sinks)

        throughput = total_processed/ ( end_time - self.start_time )
        print("System throughput: ", throughput, "items/sec" )


    # live monitor
    async def monitor(self):
        prev = 0
        while not self.stop_event.is_set():
            current = sum(n.processed for n in self.nodes.values())
            print("Throughput: ", current - prev, "items/sec")
            prev = current

            print("\n---System Stats---")

            for node in self.nodes.values():
                print(
                    f"{node.id} | "
                    f"processed = {node.processed}"
                    f"failed = {node.failed}"
                    f"queue = {node.input_queue.qsize()}"
                    )

            await asyncio.sleep(1)


input_d = {
    'nodes': {
        'inp' : {'coro': Node.input_coro, },
        'rev' : {'coro' : Node.reverse,},
        'out' : {'coro' : Node.output_coro,}
        },
    'graph' : {
        'inp' : {'rev', 'out'},
        'rev' : {'out'},
        'out' : None,
        },
    }

async def main():
    graph = Graph()

    graph.build(
        node_part = input_d['nodes'],
        graph_part = input_d['graph']
        )

    await graph.start()


asyncio.run(main())
