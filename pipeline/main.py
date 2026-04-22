import asyncio
from nodes import Node
from graph import Graph

input_d = {
    'nodes': {
        'inp' : {'coro': Node.input_coro, },
        'rev' : {'coro' : Node.reverse,},
        'out' : {'coro' : Node.output_coro,}
        },
    'graph' : {
        'inp' : {'rev'},  # earlier it was inp -> rev/out!!! rev-> out
        'rev' : {'out'}, # now it's inp->rev->out ( flow is linear )
        'out' : None,
        }
    }

async def main():
    graph = Graph()

    graph.build(
        node_part = input_d['nodes'],
        graph_part = input_d['graph']
        )
    try:
        await graph.start()
    except KeyboardInterrupt:
        print("\nStopping system...")
        graph.stop_event.set()

asyncio.run(main())
