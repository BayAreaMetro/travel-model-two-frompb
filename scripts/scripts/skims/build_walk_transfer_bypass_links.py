"""
    build_walk_transfer_bypass_links.py tap_direct_connectors mtc_tap_to_stop_connectors mtc_transit_network_node_xy output_node_file output_link_file


"""

import os,sys

tap_direct_connectors = sys.argv[1]
mtc_tap_to_stop_connectors = sys.argv[2]
mtc_transit_network_node_xy = sys.argv[3]
output_node_file = sys.argv[4]
output_link_file = sys.argv[5]

def isTap(n):
    return (n < 900000) and ((n % 100000) > 90000)

#first read node/xy and pull out taps
taps = {}
with open(mtc_transit_network_node_xy) as f:
    for line in f:
        line = line.strip()
        if len(line) == 0:
            continue
        (n,x,y) = map(int,line.split())
        if isTap(n):
            taps[n] = (x,y)

tap_links = {} #tap -> nodes (stops)
with open(mtc_tap_to_stop_connectors) as f:
    for line in f:
        line = line.strip()
        if len(line) == 0:
            continue
        (a,b) = map(int,line.split())
        if isTap(a):
            if not a in tap_links:
                tap_links[a] = {}
            tap_links[a][b] = None
        elif isTap(b):
            if not b in tap_links:
                tap_links[b] = {}
            tap_links[b][a] = None
        
#use ext space for pseudo-taps (901,000+) - leave 1,000 spots for externals
pseudo_tap_counter = 901001
tap_numbers = taps.keys()
tap_numbers.sort()
with open(output_node_file,'wb') as f:
    for tap in tap_numbers:
        (x,y) = taps[tap]
        #offset tap by 7 feet diagonally so that we are close, but not overlapping
        f.write(','.join(map(str,[pseudo_tap_counter,x+7,y+7])) + os.linesep)
        #replace coordinates with pseudo-tap number
        taps[tap] = pseudo_tap_counter
        pseudo_tap_counter += 1

with open(output_link_file,'wb') as f:
    #first write out pseudo-tap->stop links
    for tap in tap_links:
        pseudo_tap = taps[tap]
        for node in tap_links[tap]:
            f.write(','.join(map(str,[pseudo_tap,node,'TRWALK',1.0])) + os.linesep)
            f.write(','.join(map(str,[node,pseudo_tap,'TRWALK',1.0])) + os.linesep)
    #nextwrite all tap->tap connectors
    with open(tap_direct_connectors) as tf:
        for line in tf:
            line = line.strip()
            if len(line) == 0:
                continue
            connector = line.split(',')
            connector[0] = taps[int(connector[0])]
            connector[1] = taps[int(connector[1])]
            f.write(','.join(map(str,connector)) + os.linesep)
            


