import json
import sys

def detect_cycles(data):
    graph = {node: set(dependencies) for node, dependencies in data.items()}
    visited = set()
    cycle_paths = []

    def dfs(node, path, temporarily_visited):
        if node in temporarily_visited:  # Cycle found
            cycle = " -> ".join(path[path.index(node):])
            cycle_paths.append(cycle)
            return

        if node in visited:  # Already visited node, exit recursion
            return

        visited.add(node)
        temporarily_visited.add(node)

        for neighbor in graph.get(node, []):
            if neighbor == node:
                continue
            dfs(neighbor, path + [neighbor], temporarily_visited)

        temporarily_visited.remove(node)

    for node in graph:
        dfs(node, [node], set())

    return cycle_paths

def main(argv):
    with open(argv[1]) as f:
        data = json.load(f)
    # Detect cycles
    cycles = detect_cycles(data)

    # Print cycles to console
    for cycle in cycles:
        print(cycle)

if __name__ == "__main__":
    main(sys.argv)
