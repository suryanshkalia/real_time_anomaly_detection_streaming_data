import asyncio
from nodes import Node
from graph import Graph

input_d = {
    'nodes': {
        'inp' : {'coro': Node.input_coro, },
        'rev' : {'coro' : Node.reverse,},
        'win' : {'coro' : Node.window_node},
        'out' : {'coro' : Node.output_coro,},
        'anom': {'coro' : Node.anomaly_node,}
        },
    'graph' : {
        'inp' : {'rev'},  #inp->rev
        'rev' : {'win'}, #rev->window_node
        'win' : {'anom', 'out'}, # win->out
        'anom': {'out'},
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
