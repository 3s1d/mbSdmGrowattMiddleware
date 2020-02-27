#!/usr/bin/env python
# -*- coding: utf_8 -*-
"""
 Middleware to connect SDM630 indirectly to an Growatt inverter and adding an export budget onto of the readings
 This is distributed under GNU LGPL license, see license.txt
"""

import serial
from bson import json_util
import json
import math
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import threading
import time
import datetime
import struct
from datetime import timezone


#growatt TX: 01 04 00 0C 00 12 B0 04

SDMPORT = '/dev/ttyS0'
GROWATTPORT = '/dev/ttyUSB1'      #TBD
EXPORTBUDGET_W = 5530.0

logger = modbus_tk.utils.create_logger("console")

sdmbus = None
growattbus = None

running = True

today = None
sdmLastExec = 0
whImportToday = 0.0
whExportToday = 0.0
whToday_lock = threading.Lock()

def computeVA(watt, pf):
    va = [None] * min(len(watt), len(pf))
    for i in range(0, min(len(watt), len(pf))):
        va[i] = watt[i] / pf[i]
    return va

def computeVAr(watt, pf):
    var = [None] * min(len(watt), len(pf))
    for i in range(0, min(len(watt), len(pf))):
        phi = math.acos(pf[i])
        var[i] = -watt[i] * math.tan(phi)
    return var

def getMeterValues():
    global running
    global sdmbus, growattbus, sdmLastExec
    global whImportToday, whExportToday, whToday_lock
    global today

    #restart timer
    if running:
        threading.Timer(1.0, getMeterValues).start()

    # get current power measurement. do watts and power factors in one run
    sdmReg = sdmbus.execute(1, cst.READ_INPUT_REGISTERS, 12, 24, data_format='>ffffffffffff')

    # generate delta time
    current = time.time()
    dt = current - sdmLastExec
    sdmLastExec = current
    #logger.debug(dt)

    #logger.debug(sdmReg)
    if len(sdmReg) >= 12: 
        watt = [sdmReg[0], sdmReg[1], sdmReg[2]]
        pf = [sdmReg[9], sdmReg[10], sdmReg[11]]
        #logger.info(watt)

        #todo apply budget -> watt_fake
        watt_fake = watt

        va_fake = computeVA(watt_fake, pf)
        #logger.info(va)
        var_fake = computeVAr(watt_fake, pf)
        #logger.info(var)

        #todo check plausability

        #update fakesdm
        bbuf = struct.pack('>fffffffff', watt_fake[0], watt_fake[1], watt_fake[2], va_fake[0], va_fake[1], va_fake[2], va_fake[0], va_fake[1], va_fake[2])
        #logger.debug(' '.join(format(x, '03d') for x in bbuf))
        #logger.debug(' '.join(format(x, '02x') for x in bbuf))
        sbuf = struct.unpack('>%dH' % (len(bbuf)/2), bbuf)
        #logger.debug(' '.join(format(x, '04x') for x in sbuf))
        fakesdm = growattbus.get_slave(1)
        fakesdm.set_values('power', 12, sbuf)

        #mqtt stuff
        w = {'date': datetime.datetime.today()}
        for i in range(0, len(watt)):
            w['L%d'%(i+1)] = watt[i]
        #todo mqtt export
        #logger.debug(w)

        #integrate Wh
        with whToday_lock:
            if dt < 5.0:
                wh = 0.0
                for i in range(0, len(watt)):
                    wh += watt[i]
                wh *= dt/3600.0
                if wh > 0.0:
                    whImportToday += wh
                elif wh < 0.0:
                    whExportToday += wh
                #logger.debug("import %f, export %f", whImportToday, whExportToday)
                kwh = {'date': today, 'imp_kWh': whImportToday / 1000.0, 'exp_kWh': whExportToday / 1000.0}  
                #logger.debug(kwh)
        #export only now and then? every 10sec?


def main():
    """main"""
    global running
    global sdmbus, growattbus, today
    global whImportToday, whExportToday, whToday_lock

    try:
        #Connect to the slave
        sdmbus = modbus_rtu.RtuMaster(serial.Serial(port=SDMPORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0))
        sdmbus.set_timeout(5.0)
        sdmbus.set_verbose(True)
        growattbus = modbus_rtu.RtuServer(serial.Serial(GROWATTPORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0))
        growattbus.set_timeout(0.1)
        growattbus.set_verbose(True)
        growattbus.start()
        fakesdm = growattbus.add_slave(1)
        fakesdm.add_block('power', cst.ANALOG_INPUTS, 12, 18);

        logger.info("connected")
        today = datetime.datetime.today().date()

        #start collecting data
        getMeterValues()

        while True:
            time.sleep(60)
            if today != datetime.datetime.today().date():
                with whToday_lock:
                    logger.info("day change: imp=%.1fkWh exp=%.1fkWh", whImportToday/1000.0, whExportToday/1000.0)
                    #todo store into mongoDB
                    today = datetime.datetime.today().date()
                    whImportToday = 0.0
                    whExportToday = 0.0


    except modbus_tk.modbus.ModbusError as exc:
        logger.error("%s- Code=%d", exc, exc.get_exception_code())
        running = False
    except KeyboardInterrupt:
        logger.info("Interrupted by keypress")
        running = False
    finally:
        growattbus.stop()

    time.sleep(2)

if __name__ == "__main__":
    main()
