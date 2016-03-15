#!/usr/bin/env python2.7
# Author Jash Lee s905060@gmail.com

import time
import subprocess


class CassandraMetrics():

    def __init__(self):
        self.epoch_time = str(int(time.time()))

    def check_tpstats(self):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` tpstats 2>/dev/null', shell=True)
        lines = output.splitlines()
        for line in lines:
            if line != "":
                items = line.split(" ")
                items = [item for item in items if item]
                header = str(items[0].lower()).strip("_")
                if header == "pool" or header == "message":
                    pass
                else:
                    try:
                        if len(items) > 2:
                            print "cassandra.tpstats." + header + ".Active " + str(items[1]) + " " + self.epoch_time
                            print "cassandra.tpstats." + header + ".Pending " + str(items[2]) + " " + self.epoch_time
                            print "cassandra.tpstats." + header + ".Completed " + str(items[3]) + " " + self.epoch_time
                            print "cassandra.tpstats." + header + ".Blocked " + str(items[4]) + " " + self.epoch_time
                            print "cassandra.tpstats." + header + ".AllTimeBlocked " + str(items[5]) + " " + self.epoch_time
                        else:
                            print "cassandra.tpstats." + header + ".Dropped " + str(items[1]) + " " + self.epoch_time
                    except:
                        pass

    def check_netstats(self):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` netstats 2>/dev/null', shell=True)
        lines = output.splitlines()
        for line in lines:
            if line != "":
                items = line.split(" ")
                items = [item for item in items if item]
                try:
                    if len(items) > 4:
                        header = str(items[0].lower()).strip("_")
                        if header == "pool" or header == "message":
                            pass
                        else:
                            header = str(items[0]).lower() + str(items[1]).lower()
                            print "cassandra.netstats." + header + ".Active " + str(items[2]) + " " + self.epoch_time
                            print "cassandra.netstats." + header + ".Pending " + str(items[3]) + " " + self.epoch_time
                            print "cassandra.netstats." + header + ".Completed " + str(items[4]) + " " + self.epoch_time
                    else:
                        if "Attempted" in items[0]:
                            print "cassandra.netstats.ReadRepair.Attempted " + str(items[1]) + " " + self.epoch_time
                        elif "Blocking" in items[1]:
                            print "cassandra.netstats.ReadRepair.Blocking " + str(items[2]) + " " + self.epoch_time
                        elif "Background" in items[1]:
                            print "cassandra.netstats.ReadRepair.Background " + str(items[2]) + " " + self.epoch_time
                except:
                    pass

    def check_cfstats(self, keyspace):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` cfstats %s 2>/dev/null' % (keyspace), shell=True)
        lines = output.splitlines()
        table_name = ""
        for line in lines:
            try:
                if line != "":
                    line = line.split(":")
                    line[0] = line[0].replace(" ", "").replace("(", "").replace(")", "").strip('\t')
                    if str(line[0]).lower() == "keyspace":
                        pass
                    elif str(line[0]) in "--------------------------------":
                        pass
                    elif str(line[0]).lower() == "table":
                        table_name = str(line[1]).strip()
                    elif table_name != "":
                        value = str(line[1].replace(" ms.", "")).replace(" ms", "").strip()
                        print "cassandra.cfstats." + keyspace + "." + table_name + "." + str(line[0]) + " " + value + " " + self.epoch_time
                    else:
                        value = str(line[1].replace(" ms.", "").replace(" ms", "")).strip()
                        print "cassandra.cfstats." + keyspace + "." + str(line[0]).strip() + " " + value + " " + self.epoch_time
            except:
                pass

    def check_cfhistograms(self, keyspace, table):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` cfhistograms %s %s 2>/dev/null' % (keyspace, table), shell=True)
        lines = output.splitlines()
        for line in lines:
            if line != "":
                items = line.split(" ")
                items = [item for item in items if item]
                try:
                    if len(items) == 6:
                        header = str(items[0].strip("%")) + "Percentile"
                        print "cassandra.cfhistograms." + keyspace + "." + table + "." + header + ".SSTables " + str(items[1]) + " " + self.epoch_time
                        print "cassandra.cfhistograms." + keyspace + "." + table + "." + header + ".WriteLatency " + str(items[2]) + " " + self.epoch_time
                        print "cassandra.cfhistograms." + keyspace + "." + table + "." + header + ".ReadLatency " + str(items[3]) + " " + self.epoch_time
                        print "cassandra.cfhistograms." + keyspace + "." + table + "." + header + ".PartitionSize " + str(items[4]) + " " + self.epoch_time
                        print "cassandra.cfhistograms." + keyspace + "." + table + "." + header + ".CellCount " + str(items[5]) + " " + self.epoch_time
                except:
                    pass

    def check_compactionstats(self):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` compactionstats 2>/dev/null', shell=True)
        lines = output.splitlines()
        for line in lines:
            if line != "":
                items = line.split(":")
                items = [item for item in items if item]
                print "cassandra.compactionstats.PendingTask" + str(items[1]) + " " + self.epoch_time

    def check_info(self):
        output = subprocess.check_output('/usr/bin/nodetool -h `hostname -i` info 2>/dev/null', shell=True)
        lines = output.splitlines()
        for line in lines:
            if line != "":
                try:
                    items = line.split(":")
                    header = str(items[0].replace(" ", ""))
                    # items = [item for item in items if item]
                    if header in ["Load"]:
                        print "cassandra.info.LoadGB " + str(items[1]).strip(" GB") + " " + self.epoch_time
                    elif header in ["HeapMemory(MB)"]:
                        heap = items[1].split("/")
                        heap_used = heap[0]
                        heap_total = heap[1]
                        print "cassandra.info.HeapMemoryUsedMB " + str(heap_used).strip() + " " + self.epoch_time
                        print "cassandra.info.HeapMemoryTotalMB " + str(heap_total).strip() + " " + self.epoch_time
                    elif header in ["OffHeapMemory(MB)"]:
                        print "cassandra.info.OffHeapMemoryMB " + str(items[1]).strip() + " " + self.epoch_time
                    elif header in ["Exceptions"]:
                        print "cassandra.info.Exceptions " + str(items[1]).strip() + " " + self.epoch_time
                    elif header in ["KeyCache", "RowCache", "CounterCache"]:
                        metrics = items[1].split(",")
                        for metric in metrics:
                            metric = metric.split(" ")
                            values = [value for value in metric if value]
                            if "entries" in values:
                                value = values[1]
                                print "cassandra.info." + header + ".entries " + str(value).strip() + " " + self.epoch_time
                            elif "size" in values:
                                value = values[1]
                                print "cassandra.info." + header + ".sizeKB " + str(value).strip() + " " + self.epoch_time
                            elif "capacity" in values:
                                value = values[1]
                                print "cassandra.info." + header + ".capacityMB " + str(value).strip() + " " + self.epoch_time
                            elif "hits" in values:
                                value = values[0]
                                print "cassandra.info." + header + ".hits " + str(value).strip() + " " + self.epoch_time
                            elif "requests" in values:
                                value = values[0]
                                print "cassandra.info." + header + ".requests " + str(value).strip() + " " + self.epoch_time
                except:
                    pass

    def get_user_keyspaces(self):
        output = subprocess.check_output('cqlsh `hostname -i` -e "DESCRIBE keyspaces;"', shell=True)
        lines = output.splitlines()
        keyspaces = []
        system_keyspaces = ['system_auth', 'system', 'system_distributed', 'system_traces']
        for line in lines:
            if line != "":
                keyspaces = line.split(" ")
                keyspaces = [keyspace for keyspace in keyspaces if (keyspace not in system_keyspaces)]
                keyspaces = [keyspace for keyspace in keyspaces if keyspace]
        return keyspaces

    def get_user_tables(self, keyspace):
        output = subprocess.check_output('cqlsh `hostname -i` -e "Use %s; DESCRIBE tables;"' % (keyspace), shell=True)
        lines = output.splitlines()
        tables = []
        for line in lines:
            if line != "":
                tables = line.split(" ")
                tables = [table for table in tables if table]
        return tables

if __name__ == "__main__":
    metric_checker = CassandraMetrics()
    metric_checker.check_tpstats()
    metric_checker.check_netstats()
    metric_checker.check_compactionstats()
    metric_checker.check_info()
    user_keyspaces = metric_checker.get_user_keyspaces()
    for user_keyspace in user_keyspaces:
        metric_checker.check_cfstats(user_keyspace)
        user_tables = metric_checker.get_user_tables(user_keyspace)
        for user_table in user_tables:
            metric_checker.check_cfhistograms(user_keyspace, user_table)
