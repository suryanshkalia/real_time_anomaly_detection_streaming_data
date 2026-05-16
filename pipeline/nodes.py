import asyncio
import random
import uuid
from constants import STOP
import time

# right now its batch style execution that i will turn into real-time flow engine
# now this is a dag streaming engine/ but not real-time like we need
# right now this is just high-thrp DAG executor not really what i need
# introducing watermarking helped my system to advance according to
# stream progress and not machine run_time and watermarks are useless unless
#diroder exists, watermarking is right now just okay, it works well but needs to do more in future

class Node:
    def __init__(self, id, coro, workers=3, queue_size=2, retries=3):
        self.id = id
        self.coro = coro
        self.workers = workers
        self.retries = retries
        self.state = {}
        self.processed = 0
        self.failed = 0
        self.first_item_time = None  # time of fisrt item processed
        self.last_item_time = None   # time of last item processed

        self.inputs = set()  #dependencies( b takes from a so b depends upon a)
        self.outputs = set()  #downstream nodes( where the data is flowing towards)

        self.input_queue = asyncio.PriorityQueue(maxsize=queue_size) # now if downstream is slow then producer will
        # try to be prodcue slowly, it's basic backpressure handling'


    @classmethod
    #producer
    async def input_coro(cls, grph=None, output_queue = None, stop_event = None):
        try:
            while not stop_event.is_set():
                sleep_time, pressure = grph.calculate_sleep() # get sleep, pressure

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time) # slow down the producer
                    # according to the sleep time recieved

                item = {
                    "id" : str(uuid.uuid4()),
                    "ts" : time.time() - random.uniform(0, 2), #random so that some events arrive late, watermakr advances

                    #add some fake telemetry, our prodcuer is quite stable so z-score is rarely spiking
                    "cpu" : random.uniform(20, 60),
                    "memory" : random.uniform(30, 70),
                    "latency" : random.uniform(50, 150)
                    }

                if random.random() < 0.02:
                    item["latency"] = random.uniform(500, 1500) # to insert anomaly, normal latency -> 30ms but this one can be more than enough for anomaly, this is done manually to flag a anomaly

                deadline = item["ts"] + grph.MAX_LATENCY
                priority = deadline  # earliest deadline processed first

                await output_queue.put((priority, item))
                print("Produced: ", item["id"])
                print(f"[Producer] pressure = {pressure:.2f}, sleep = {sleep_time:.3f}")

        except asyncio.CancelledError:
            print("Producer Stopped")
            raise
        finally:
            print("Producer sending STOP")
            await output_queue.put((float('inf'), STOP))

    @classmethod
    async def reverse(cls, grph=None, node=None,  input_data=None):
        if input_data is STOP:
            return STOP
        await asyncio.sleep(0.05) # without thuis the revrerse is too fast, queues wont be filled
        # properly, producer will rarely block so downstream is too fast
        return {
            **input_data
            }

    @classmethod
    async def output_coro(cls, grph=None, node = None, input_data=None):
        if input_data is STOP:
            print("Output node recieved Stop, shutting down")
            return input_data
        print("output: ", input_data)

    #every incomgin event ebters window, old are removed, node then emits
    #current active events and count of events
    @classmethod
    async def window_node(cls, grph=None, node=None, input_data=None):
        if input_data is STOP:
            return STOP

        if "window" not in node.state:
            node.state["window"] = []

        window = node.state["window"]

        WINDOW_SIZE = 1.0

        window.append(input_data)

        watermark = grph.current_watermark # time.time() replaced with this, as that was not better for sgtream processing



        window[:] = [
            item for item in window
            if item["ts"] >= watermark - WINDOW_SIZE # keep items newer than window boundary
            ]

        latencies = [e["latency"] for e in window]

        avg_latency = sum(latencies) / len(latencies)

        return {
            "id" : str(uuid.uuid4()),
            "ts" : watermark,
            "watermark" : watermark,
            "count" : len(window),
            "avg_latency" : avg_latency,
            "max_latency" : max(latencies),
            "events" : window.copy()
            }

    @classmethod
    async def anomaly_node(cls=None, grph=None, node=None, input_data=None):
        if input_data is STOP:
            return STOP

        current = input_data["avg_latency"] # our metric is latency, get the  latest value

        alpha = 0.2 # smoothing factor, higher value= reacts faster, more sensitive, more noise, alpha controls memory
        #lower alpha = smoother, slower adaptation, more stable, 0.2 is balanced

        if "ewma" not in node.state:

           node.state["ewma"] = current
           node.state["varience"] = 0

           return {
               **input_data,
               "ewma" : current,
               "std" : 0,
               "z_score" : 0,
               "anomaly" : False
               }

        prev_ewma = node.state["ewma"]
        prev_varience = node.state["varience"]

        ewma = (
           alpha*current + ( 1- alpha ) * prev_ewma
           )

        varience = (
           alpha * ((current - ewma)**2) + ( 1- alpha) * prev_varience
           )

        std = varience**0.5

        z_score = (
           (current - ewma) / std
           if std > 0 else 0
           )

        anomaly = abs(z_score) > 3

        node.state["ewma"] = ewma
        node.state["varience"] = varience

        return {
           **input_data,
           "ewma" : ewma,
           "std" : std,
           "z_score" : z_score,
           "anomaly" : anomaly
           }


