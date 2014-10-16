from collections import deque
from pipeline import dfs_stages

def bfs_topo_sort(root_stage):
    '''A breadth first search for topological sorting, first described by Kahn (1962).'''
    # Algorithm follows Wikipedia: http://en.wikipedia.org/wiki/Topological_sorting
    edges = set()
    for n in dfs_stages(root_stage):
        for m in n.dependents:
            edges.add((n,m))
    
    done = []
    todo = deque([root_stage])
    while len(todo) > 0:
        n = todo.popleft()
        done.append(n)
        for m in n.dependents:
            edges.remove((n, m))
            m_ready = True
            for p in m.prereqs:
                if (p, m) in edges:
                    m_ready = False
                    break
            if m_ready:
                todo.append(m)
    assert len(edges) == 0, "Dependency graph has a cycle"
    return done
