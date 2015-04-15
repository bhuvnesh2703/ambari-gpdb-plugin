#!/usr/bin/env python
import re
from resource_management.libraries.functions.version import _normalize

def format_stack_version(input):
  """
  Copy of ambari-common/../version.py, as method name is different for PHD and HDP.
  :param input: Input string, e.g. "2.2" or "GlusterFS", or "2.0.6.GlusterFS"
  :return: Returns a well-formatted stack version of the form #.#.#.# as a string.
  """
  if input:
    input = re.sub(r'^\D+', '', input)
    input = re.sub(r'\D+$', '', input)
    input = input.strip('.')

    strip_dots = input.replace('.', '')
    if strip_dots.isdigit():
      normalized = _normalize(str(input))
      if len(normalized) == 2:
        normalized = normalized + [0, 0]
      elif len(normalized) == 3:
        normalized = normalized + [0, ]
      normalized = [str(x) for x in normalized]   # need to convert each number into a string
      return ".".join(normalized)
  return ""

