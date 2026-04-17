import asyncio
import uuid


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
    async def input_coro(cls, Processor=None, output_queue = None, stop_event = None):
        # use while not stop_event.is_set() and not range for infinite
           for i in range(10): # controlled input
               item_id = str(uuid.uuid4()) # generating unique id per item
               await output_queue.put(item_id)
               print("produced:", item_id)

           stop_event.set() # to block after 10 items

    @classmethod
    async def reverse(cls, Processor=None, input_data=None):
        await asyncio.sleep(0.5) # without thuis the revrerse is too fast, queues wont be filled
        # properly, producer will rarely block so downstream is too fast
        return input_data[::-1]

    @classmethod
    async def output_coro(cls, Processor=None, input_data=None):
        print("output: ", input_data)


