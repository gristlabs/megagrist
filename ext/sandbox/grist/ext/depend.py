import depend_base
from depend_base import *

import logging
log = logging.getLogger(__name__)
log.error("WEEE0")

class Graph(depend_base.Graph):
  """
  Represents the dependency graph for all data in a grist document.
  """
  def __init__(self):
    log.error("WEEE1")
    super(Graph, self).__init__()
