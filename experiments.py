import json
import os
import time

import csv
from subprocess import *


def jarWrapper(package, args):
    last_line = ''
    with Popen(['java', '-Xmx10G', '-cp',
                os.path.dirname(os.path.abspath(__file__)) + '/target/queries-1.0-SNAPSHOT.jar',
                package] + list(args), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            last_line = line
    return last_line


package_az_fi_apprx = 'de.lindener.analysis.amazon.AZFrequentItemsApproximate'
package_az_fi_exact = 'de.lindener.analysis.amazon.AZFrequentItemsExact'
package_az_hll_apprx = 'de.lindener.analysis.amazon.AZHllApproximate'

package_wt_fi_apprx = 'de.lindener.analysis.wikitrace.WTFrequentItemsApproximate'

package_il_fi_apprx = 'de.lindener.analysis.impressions.ILFrequentItemsApproximate'
package_il_fi_exact = 'de.lindener.analysis.impressions.ILFrequentItemsExact'

map_sizes = [16384, 32768, 65536, 131072]


def runAZFIApprx(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_az_fi_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}


def runAZFIExact(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_az_fi_exact, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}


def runWTFIApprx(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_wt_fi_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}

def runILFIExact(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_il_fi_exact, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}

def runILFIApprx(emit_min=1000, bound=10000000, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_il_fi_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}


def runHLLApprx(emit_min=1000):
    args = ['--emit-min', str(emit_min)]
    start = time.time()
    result = jarWrapper(package_az_hll_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'runtime': runtime, 'resultPath': result['resultPath']}


def runHLLTests():
    fieldnames = ['runtime', 'resultPath']
    with open('HLL_APPRX.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        result = runHLLApprx(emit_min=10000)
        writer.writerow(result)

def runFITests():
    fieldnames = ['mapSize', 'runtime', 'resultPath']
    with open('WTFI_APPRX.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for mapSize in map_sizes:
            result = runWTFIApprx(emit_min=10000, map_size=mapSize)
            writer.writerow(result)

    # with open('AZFI_APPRX.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for mapSize in map_sizes:
    #         result = runAZFIApprx(map_size=mapSize)
    #         writer.writerow(result)

    # with open('ILFI_APPRX.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for mapSize in map_sizes:
    #         result = runILFIApprx(map_size=mapSize)
    #         writer.writerow(result)

    # with open('AZFI_EXACT.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     result = runAZFIExact()
    #     writer.writerow(result)
    #
    # with open('ILFI_EXACT.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     result = runILFIExact()
    #     writer.writerow(result)


runFITests()
# runHLLTests()
