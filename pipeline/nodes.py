import asyncio
import random
import uuid
import copy
import time
from constants import STOP
from river import anomaly
from river import preprocessing

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

                    #keys for keyed streaming
                    "service" : random.choice(
                        [
                            "auth",
                            "payments",
                            "search",
                            "recommendation"
                            ]
                        ),

                    #add some fake telemetry, our prodcuer is quite stable so z-score is rarely spiking
                    "cpu" : random.uniform(20, 60),
                    "memory" : random.uniform(30, 70),
                    "latency" : random.uniform(50, 150)
                    }

                if random.random() < 0.08:
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
        print(
            f"""
            SERVICE : {input_data['service']}
            AVG LATENCY : {input_data['avg_latency']:.2f}
            ANOMALY SCORE : {input_data['anomaly_score']:.4f}
            ANOMALY: {input_data['anomaly']}
            """
            )

    #every incomgin event ebters window, old are removed, node then emits
    #current active events and count of events
    @classmethod
    async def window_node(cls, grph=None, node=None, input_data=None):
        if input_data is STOP:
            return STOP

        key = input_data["service"]

        if "windows" not in node.state:
            node.state["windows"] = {}

        windows = node.state["windows"]

        if "seen" not in node.state:
            node.state["seen"] = set()

        seen = node.state["seen"]

        if input_data["id"] in seen:
            return None

        seen.add(input_data["id"])

        if len(seen) > 10000:
            seen.clear()

        if key not in windows:
            windows[key] = []

        window = windows[key]

        WINDOW_SIZE = 0.75

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
            "service" : key,
            "watermark" : watermark,
            "count" : len(window),
            "avg_latency" : avg_latency,
            "max_latency" : max(latencies),
            "events" : copy.deepcopy(window),
            "raw_latency" : input_data["latency"]
            }
    #now wvery service will get indep. windows, isolated telemetry, indep. aggregation

    @classmethod
    async def anomaly_node(cls=None, grph=None, node=None, input_data=None):

        if input_data is STOP:
            return STOP

        key = input_data["service"]

        #initialize per-service models
        if "models" not in node.state:
            node.state["models"] = {}

        models = node.state["models"]

        # creating model first time this service appears
        if key not in models:

            models[key] = {
                "scaler" : preprocessing.StandardScaler(),
                "detector" : anomaly.HalfSpaceTrees(
                    n_trees = 5,
                    height = 3,
                    window_size = 25,
                    seed = 42
                    )
                }

        state = models[key]

        scaler = state["scaler"]
        detector = state["detector"]

        #feature vector
        x = {
            "avg_latency" : input_data["avg_latency"],
            "max_latency" : input_data["max_latency"],
            "count" : input_data["count"],
            "raw_latency" : input_data["raw_latency"]
            }

        #online scaling
        scaler.learn_one(x)
        x_scaled = scaler.transform_one(x)

        #anomaly score
        score = detector.score_one(x_scaled)

        #online learning update
        detector.learn_one(x_scaled)

        #threshold
        anomaly_detected = score > 0.55

        return {
            **input_data,
            "anomaly_score" : round(score, 4),
            "anomaly": anomaly_detected
            }


