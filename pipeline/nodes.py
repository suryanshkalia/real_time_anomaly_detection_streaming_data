import asyncio
import uuid
from constants import STOP

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

        self.input_queue = asyncio.Queue(maxsize=queue_size) # now if downstream is slow then producer will
        # try to be prodcue slowly, it's basic backpressure handling'


    @classmethod
    #producer
    async def input_coro(cls, grph=None, output_queue = None, stop_event = None):
        try:
            while True:
                item_id = str(uuid.uuid4())

                await output_queue.put(item_id)
                print("Produced: ", item_id)

                pressure = grph.get_global_pressure()

                # rate of producer controlled via backpressure signal
                if pressure > 0.8:
                    sleep_time = 0.3
                elif pressure > 0.5:
                    sleep_time = 0.15
                else:
                    sleep_time = 0.01

                print(f"[Producer] pressure={pressure:.2f}, sleep={sleep_time}")
                await asyncio.sleep(sleep_time)

        finally:
            print("Producer Stopped Sedning")
            await output_queue.put(STOP)

    @classmethod
    async def reverse(cls, grph=None, input_data=None):
        if input_data == STOP:
            return STOP
        await asyncio.sleep(0.5) # without thuis the revrerse is too fast, queues wont be filled
        # properly, producer will rarely block so downstream is too fast
        return input_data[::-1]

    @classmethod
    async def output_coro(cls, grph=None, input_data=None):
        if input_data == STOP:
            print("Output node recieved Stop, shutting down")
            return STOP
        print("output: ", input_data)


