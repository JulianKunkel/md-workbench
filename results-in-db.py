#!/usr/bin/env python3

import sqlite3
import sys
import re

import traceback
import logging

def parseHeader(file):
  head = file.readline()
  keys = {}
  prefix = ""
  for line in file:
    if line == "\n":
      if(prefix != ""):
        break;
      prefix="plugin_"
      continue
    if line.startswith("\t"):
      line = line.strip()
      if line.find("=") != -1:
        (k, v) = line.split("=")
        k = k.replace("-", "_")
        keys[prefix + k] = v;
      continue
  return keys

def findColumns(file):
  f = open(file, "r")
  r = parseHeader(f)
  f.close()
  return list(r.keys())

def import_file(file):
  print("Importing " + file)
  f = open(file, "r")
  header = parseHeader(f)
  # parse results
  regex = re.compile("((?P<rank>[0-9]+): )?(?P<phase>[a-z]+).* (?P<obj>[0-9.]+) obj/s.*?( (?P<mib>[0-9.]+) Mib/s)? \((?P<errs>[0-9]+) errs")
  iteration = 0
  for line in f:
    m = regex.match(line)
    if m:
      process = m.group('rank')
      if not process:
        process = "NULL"
        iteration = iteration + 1
      phase = m.group('phase')[0]
      obj_per_s = m.group('obj')
      throughput_MiB = m.group('mib')
      if not throughput_MiB:
        throughput_MiB = "NULL";
      errors = m.group('errs')

      keys = ",".join(list(header.keys()));
      values = ",".join( [ "\"" + x + "\"" for x in list(header.values())] );

      sql = "INSERT into r (prefix, file, phase, iteration, process, errors, obj_per_s, throughput_MiB, " + keys + ") VALUES (\"%s\", \"%s\", \"%s\", %s,%s,%s,%s,%s,%s)" % (prefix, file, phase, iteration - 1, process, errors, obj_per_s, throughput_MiB, values)
      # print(sql)
      conn.execute(sql)

  f.close()

conn = sqlite3.connect('results.db')

prefix = sys.argv[1]
files = sys.argv[2:]

print("Importing : " + prefix)

otherColumns = findColumns(sys.argv[2])

sql = "CREATE TABLE IF NOT EXISTS r( prefix text, file text, iteration int, phase char, process int, errors int, obj_per_s float, throughput_Mib float, " + " float , ".join(otherColumns) + " float )"
#print(sql)
conn.execute(sql)
#except Exception as e:
#  logging.error(traceback.format_exc())

for f in files:
  import_file(f)


conn.commit()
conn.close()
