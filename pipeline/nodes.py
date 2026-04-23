import asyncio
import uuid
from constants import STOP
import time

# right now its batch style execution that i will turn into real-time flow engine
# now this is a dag streaming engine/ but not real-time like we need
# right now this is just high-thrp DAG executor not really what i need

class Node:
    def __init__(self, id, coro, workers=3, queue_size=2, retries=3):
        self.id = id
        self.coro = coro
        self.workers = workers
        self.retries = retries
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
                    "ts" : time.time() # time stamp
                    }

                priority = -item["ts"]

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
    async def reverse(cls, grph=None, input_data=None):
        if input_data == STOP:
            return STOP
        await asyncio.sleep(0.5) # without thuis the revrerse is too fast, queues wont be filled
        # properly, producer will rarely block so downstream is too fast
        return {
            "id" : input_data["id"],
            "ts" : input_data["ts"],
            }

    @classmethod
    async def output_coro(cls, grph=None, input_data=None):
        if input_data == STOP:
            print("Output node recieved Stop, shutting down")
            return STOP
        print("output: ", input_data)


