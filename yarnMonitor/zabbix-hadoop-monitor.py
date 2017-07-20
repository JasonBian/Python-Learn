# coding=utf-8
# !/usr/bin/python

import sys
import argparse
import urllib2
import re
import traceback

SERVICE_UP = 1
SERVICE_DOWN = 0
PTRN_TAG = re.compile('<[^>]+>')


class Job(object):
    def __init__(self, args):
        self.args = args

    def run(self):
        try:
            getattr(self, 'collect_%s' % self.args.type)()
        except Exception:
            traceback.print_exc()
            print 'SERVICE_DOWN'
        else:
            print SERVICE_UP
            pass

    def request(self, url):
        # logging.info('Request: %s' % url)
        f = urllib2.urlopen(url)
        content = f.read()
        f.close()
        return content

    def collect_namenode(self):

        content = self.request('http://%s:%d/dfshealth.jsp' % (self.args.namenode_host, self.args.namenode_port))
        print content
        result = {}

        mo = re.search('([0-9]+) files and directories, ([0-9]+) blocks', content)
        result['file_count'] = mo.group(1)
        result['block_count'] = mo.group(2)

        mo = re.search('Heap Memory used ([0-9.]+ [KMGTP]?B).*?Max Heap Memory is ([0-9.]+ [KMGTP]?B).', content)
        result['heap_used'] = self.regulate_size(mo.group(1))
        result['heap_total'] = self.regulate_size(mo.group(2))

        for dfstable in content.split('\n'):
            if 'Configured Capacity' in dfstable:
                break

        dfstable = re.sub('<tr[^>]*>', '\n', dfstable)
        dfstable = PTRN_TAG.sub('', dfstable)
        dfsmap = {}
        for line in dfstable.split('\n'):
            if re.findall("[a-z]", line) and re.findall("[0-9]", line) and not re.findall("Decommissioned", line):
                try:
                    k, v = line.split(':')
                    dfsmap[k.strip()] = v.strip()
                except ValueError:
                    traceback.print_exc()
            elif re.findall("Decommissioned", line):
                try:
                    c = line.split('(')[0]
                    k, v = c.split(':')
                    dfsmap[k.strip()] = v.strip()
                except ValueError:
                    traceback.print_exc()

        print dfsmap

        result['dfs_capacity'] = self.regulate_size(dfsmap['Configured Capacity'])
        result['dfs_alreadyused'] = self.regulate_size(dfsmap['DFS Used'])
        result['dfs_used_other'] = self.regulate_size(dfsmap['Non DFS Used'])
        result['dfs_remaining'] = self.regulate_size(dfsmap['DFS Remaining'])
        result['node_alive'] = dfsmap['Live Nodes']
        result['node_dead'] = dfsmap['Dead Nodes']
        result['node_decom'] = dfsmap['Decommissioning Nodes']
        result['block_under'] = dfsmap['Number of Under-Replicated Blocks']
        a = float(result['dfs_alreadyused']) / result['dfs_capacity'] * 100
        result['dfs_used'] = round(a, 2)

        it = self.args.item
        if not it:
            print result
        else:
            print result[it]

    def collect_datanode(self):

        content = self.request(
            'http://%s:%d/dfsnodelist.jsp?whatNodes=LIVE' % (self.args.namenode_host, self.args.namenode_port))

        lines = iter(content.split('\n'))
        for line in lines:
            if line.startswith('<tr class="headerRow">'):
                break
        jthead = line

        for line in lines:
            if line.startswith('<tr') \
                    and self.args.host in line:
                break
        jtbody = re.sub('<table[^>]*>.*?</table>', '', line)

        iter_head = re.finditer('<th[^>]*>(.*?)(?=<th|$)', jthead)
        iter_body = re.finditer('<td[^>]*>(.*?)(?=<td|$)', jtbody)
        jtmap = {}
        ptrn_quote = re.compile(r'\((.*?)\)')
        try:
            for mo_head in iter_head:
                mo_body = iter_body.next()

                k = PTRN_TAG.sub('', mo_head.group(1))
                if '(%)' in k:
                    continue

                mo = ptrn_quote.search(k)
                k = ptrn_quote.sub('', k).strip()
                v = PTRN_TAG.sub('', mo_body.group(1)).strip()

                if mo is not None:
                    jtmap[k] = '%s %s' % (v, mo.group(1))
                else:
                    jtmap[k] = v
        except StopIteration:
            pass

        result = {}
        result['dfs_capacity'] = self.regulate_size(jtmap['Configured Capacity'])
        result['dfs_alreadyused'] = self.regulate_size(jtmap['Used'])
        result['dfs_used_other'] = self.regulate_size(jtmap['Non DFS Used'])
        result['dfs_remaining'] = self.regulate_size(jtmap['Remaining'])
        result['block_count'] = jtmap['Blocks']
        a = float(result['dfs_alreadyused']) / result['dfs_capacity'] * 100

        result['dfs_used'] = round(a, 2)

        it = self.args.item
        if not it:
            print result
        else:
            print result[it]

    def regulate_size(self, size):
        try:
            size, unit = size.split()
            size = float(size)
        except ValueError:
            return 0

        if unit == 'KB':
            size = size * 1024
        elif unit == 'MB':
            size = size * 1024 ** 2
        elif unit == 'GB':
            size = size * 1024 ** 3
        elif unit == 'TB':
            size = size * 1024 ** 4
        elif unit == 'PB':
            size = size * 1024 ** 5

        return int(round(size))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hadoop metrics collector for Zabbix.')

    parser.add_argument('-t', '--type', required=True, help='collector type',
                        choices=['namenode', 'datanode', 'jobtracker', 'tasktracker'])

    parser.add_argument('--namenode-host', default='192.168.10.84')
    parser.add_argument('--namenode-port', type=int, default=50070)

    parser.add_argument('-z', '--zabbix-home', default='/usr/local/zabbix-agent-ops')
    parser.add_argument('-s', '--host', required=True, help='hostname recognized by zabbix')
    parser.add_argument('-i', '--item', help="for namenode : 'block_count','dfs_remaining','dfs_used_other','heap_total','node_alive','heap_used','dfs_capacity',\
'node_decom','block_under','node_dead','file_count','dfs_used' ; for datanode : 'dfs_capacity','dfs_remaining','dfs_usedother','block_count','dfs_used' ")

    args = parser.parse_args()

    Job(args).run()
