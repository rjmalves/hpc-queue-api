# import asyncio

# from app.internal.terminal import run_terminal


# async def main():
#     cod, res = await run_terminal(["ls ./a"])
#     print(cod)
#     print(res)


# asyncio.run(main())


# SGE:
# Uso de CPU, MEM e IO:
# - CPU: valores acumulados em segundos
# - MEM: valores acumulados de uso de memória em GB * segundos de CPU
# - IO:  valores acumulados em quantidade de dados transferidos
#  - IOW: valores de espera acumulados para IO em segundos

# qacct -j ...

import xml.etree.ElementTree as ET

mytree = ET.parse("qstat.xml")
myroot = mytree.getroot()
for job_xml in myroot[0]:
    jobId = job_xml.find("JB_job_number").text
    name = job_xml.find("JB_name").text

myroot[0][0].find("JB_name").text
