import csv
import json
import os
import time
from subprocess import *


def jarWrapper(package, args):
    last_line = ''
    with Popen(['java', '-Xmx12G', '-cp',os.path.dirname(os.path.abspath(__file__)) + '/target/queries-1.0-SNAPSHOT.jar',
                package] + list(args), stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            last_line = line
    return last_line


package_az_fi_apprx = 'de.lindener.analysis.amazon.AZFrequentItemsApproximate'
package_az_fi_exact = 'de.lindener.analysis.amazon.AZFrequentItemsExact'
package_az_hll_apprx = 'de.lindener.analysis.amazon.AZHllApproximate'

package_il_fi_apprx = 'de.lindener.analysis.impressions.ILFrequentItemsApproximate'
package_il_fi_exact = 'de.lindener.analysis.impressions.ILFrequentItemsExact'

map_sizes = [128, 1024, 4096,16384]

def runAZFIApprx(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_az_fi_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'mapSize': map_size, 'runtime': runtime, 'resultPath': result['resultPath']}

def runAZHLLApprx(emit_min=1000, bound=0):
    args = ['--emit-min', str(emit_min), '--bound', str(bound)]
    start = time.time()
    result = jarWrapper(package_az_hll_apprx, args)
    end = time.time()
    result = json.loads(result)
    runtime = end - start
    return {'runtime': runtime, 'resultPath': result['resultPath']}


def runAZFIExact(emit_min=1000, bound=0, map_size=128):
    args = ['--emit-min', str(emit_min), '--bound', str(bound), '--map-size', str(map_size)]
    start = time.time()
    result = jarWrapper(package_az_fi_exact, args)
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


def runHLLTests():
    emit_min = 30000
    fieldnames = ['runtime','resultPath']
    with open('AZHLL_APPRX.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        result = runAZHLLApprx(emit_min=emit_min)
        writer.writerow(result)


def runFITests():
    emit_min = 10000
    fieldnames = ['mapSize', 'runtime', 'resultPath']
    with open('AZFI_APPRX.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for mapSize in map_sizes:
            print("Experiment with map_size: " + str(mapSize))
            result = runAZFIApprx(emit_min=emit_min,map_size=mapSize)
            writer.writerow(result)

    # with open('ILFI_APPRX.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for mapSize in map_sizes:
    #         result = runILFIApprx(emit_min=emit_min,map_size=mapSize)
    #         writer.writerow(result)

    with open('AZFI_EXACT.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        result = runAZFIExact(emit_min=emit_min)
        writer.writerow(result)
    #
    # with open('ILFI_EXACT.csv', 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     result = runILFIExact()
    #     writer.writerow(result)


runHLLTests()
