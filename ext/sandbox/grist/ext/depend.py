import depend_base
from depend_base import *
import sqlite3

import logging
log = logging.getLogger(__name__)
log.error("WEEE0")

#
#Node1 <table_id, col_id>
#Node2 <table_id, col_id>
#Relation

class Graph(object):
  """
  Represents the dependency graph for all data in a grist document.
  """
  def __init__(self):
    self._conn = sqlite3.connect(":memory:")
    self._conn.execute('''
      CREATE TABLE IF NOT EXISTS _gristsys_Deps (
        out_table_id TEXT,
        out_col_id TEXT,
        in_table_id TEXT,
        in_col_id TEXT
      )
    ''')

    # Maps ROWID of edge to a relation object.
    self._edges = {}

    # # The set of all Edges, i.e. the complete dependency graph.
    # self._all_edges = set()

    # # Map from node to the set of edges having it as the in_node (i.e. edges to dependents).
    # self._in_node_map = {}

    # # Map from node to the set of edges having it as the out_node (i.e. edges to dependencies).
    # self._out_node_map = {}

  def dump_graph(self):
    """
    Print out the graph to stdout, for debugging.
    """
    cursor = self._conn.cursor()
    for edge in cursor.execute('SELECT * FROM _gristsys_Deps').fetchiter():
      print("edge", edge)

  def add_edge(self, out_node, in_node, relation):
    """
    Adds an edge to the global dependency graph: out_node depends on in_node, i.e. a change to
    in_node should trigger a recomputation of out_node.
    """
    cursor = self._conn.cursor()
    cursor.execute('''
      INSERT INTO _gristsys_Deps (
        out_table_id,
        out_col_id,
        in_table_id,
        in_col_id
      ) VALUES (?, ?, ?, ?)
      ''',
      out_node + in_node
    )
    self._edges[cursor.lastrowid] = relation

  def clear_dependencies(self, out_node):
    """
    Removes all edges which affect the given out_node, i.e. all of its dependencies.
    """
    cursor = self._conn.cursor()
    rows = cursor.execute('''
      SELECT rowid FROM _gristsys_Deps
      WHERE out_table_id=? AND out_col_id=?
      ''',
      out_node
    )
    for row in rows:
      relation = self._edges.pop(row[0])
      if relation:
        relation.reset_all()
    cursor.execute('''
      DELETE FROM _gristsys_Deps
      WHERE out_table_id=? AND out_col_id=?
      ''',
      out_node
    )

  def reset_dependencies(self, node, dirty_rows):
    """
    For edges the given node depends on, reset the given output rows. This is called just before
    the rows get recomputed, to allow the relations to clear out state for those rows if needed.
    """
    cursor = self._conn.cursor()
    rows = cursor.execute('''
      SELECT rowid FROM _gristsys_Deps
      WHERE out_table_id=? AND out_col_id=?
      ''',
      node
    )
    for row in rows:
      relation = self._edges.get(row[0])
      if relation:
        relation.reset_rows(dirty_rows)

  def remove_node_if_unused(self, node):
    """
    Removes the given node if it has no dependents. Returns True if the node is gone, False if the
    node has dependents.
    """
    cursor = self._conn.cursor()
    row = cursor.execute('''
      SELECT COUNT(*) FROM _gristsys_Deps
      WHERE in_table_id=? AND in_col_id=?
      ''',
      node
    ).fetchone()
    if row[0] > 0:
      return False
    self.clear_dependencies(node)
    return True


  def invalidate_deps(self, dirty_node, dirty_rows, recompute_map, include_self=True):
    """
    Invalidates the given rows in the given node, and all of its dependents, i.e. all the nodes
    that recursively depend on dirty_node. If include_self is False, then skips the given node
    (e.g. if the node is raw data rather than formula). Results are added to recompute_map, which
    is a dict mapping Nodes to sets of rows that need to be recomputed.

    If dirty_rows is ALL_ROWS, the whole column is affected, and dependencies get recomputed from
    scratch. ALL_ROWS propagates to all dependent columns, so those also get recomputed in full.
    """
    to_invalidate = [(dirty_node, dirty_rows)]

    while to_invalidate:
      dirty_node, dirty_rows = to_invalidate.pop()
      if include_self:
        if recompute_map.get(dirty_node) == ALL_ROWS:
          continue
        if dirty_rows == ALL_ROWS:
          recompute_map[dirty_node] = ALL_ROWS
          # If all rows are being recomputed, clear the dependencies of the affected column. (We add
          # dependencies in the course of recomputing, but we can only start from an empty set of
          # dependencies if we are about to recompute all rows.)
          self.clear_dependencies(dirty_node)
        else:
          out_rows = recompute_map.setdefault(dirty_node, SortedSet())
          prev_count = len(out_rows)
          out_rows.update(dirty_rows)
          # Don't bother recursing into dependencies if we didn't actually update anything.
          if len(out_rows) <= prev_count:
            continue

      include_self = True


      cursor = self._conn.cursor()
      rows = cursor.execute('''
        SELECT rowid, out_table_id, out_col_id FROM _gristsys_Deps
        WHERE in_table_id=? AND in_col_id=?
        ''',
        dirty_node
      )
      for row in rows:
        rowid, out_table_id, out_col_id = row
        out_node = Node(out_table_id, out_col_id)
        relation = self._edges.get(rowid)
        if not relation:
          continue

        affected_rows = relation.get_affected_rows(dirty_rows)

        # Previously this was:
        #   self.invalidate_deps(edge.out_node, affected_rows, recompute_map, include_self=True)
        # but that led to a recursion error, so now we do the equivalent
        # without actual recursion, hence the while loop
        to_invalidate.append((out_node, affected_rows))
