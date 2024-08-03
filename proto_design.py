import csv
import json
import asyncio
import httpx
import logging
import sys
import string
import random

import os

from httpx import ReadTimeout, RemoteProtocolError

logstream_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(handlers=[logstream_handler],
                    format='%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

TARGET_BASE_URL = 'https://api.data.belajar.id/data-portal-backend/v1/master-data/'

MAIN_AREA = '360'

def unnest_data(nested_data:list) -> list:
    flatten_data = [datum for array_datum in nested_data for datum in array_datum]
    return flatten_data

def randstr(n:int) -> str:
    chars = string.ascii_letters+string.digits
    randout = ''.join(random.choice(chars) for i in range(n))
    return randout

def paginator(items:list, items_per_page:int):
    pages = [items[item:item+items_per_page] for item in range(0, len(items), items_per_page)]
    return pages

async def job_aggregator(job_list):
    job_agg = await asyncio.gather(*job_list)
    return job_agg

def parse_subarea(decoded_content:str, parent:str) -> list:
    area_list = []
    for area in json.loads(decoded_content)['data']:
        data = area['district']
        data['kodeIndukWilayah'] = parent
        area_list.append(data)

    return area_list

def fetch_subarea(parent_area:str) -> list:
    parent_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{parent_area}/descendants?sortBy=bentuk_pendidikan&sortDir=asc'
    fetch_raw = httpx.get(parent_url).content.decode('UTF-8')
    data = parse_subarea(fetch_raw, parent_area)
    
    return data

def crawl_subareas(parent_areas:list) -> list:    
    async def _crawl(_area:str) -> dict:
        async with httpx.AsyncClient(timeout=None) as client:
            parent_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{_area}/descendants?sortBy=bentuk_pendidikan&sortDir=asc'
            fetch_raw = await client.get(parent_url)
            resp = fetch_raw.content.decode('UTF-8')
            parsed_data = parse_subarea(resp,_area)
            
            return parsed_data
    joblist = [_crawl(area) for area in parent_areas]
    grab_data = asyncio.run(job_aggregator(joblist))

    repack_data = unnest_data(grab_data)
    
    return repack_data

def fetch_schlist(lv3_codearea:str) -> list:
    metadata_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{lv3_codearea}'
    data_url = f'{TARGET_BASE_URL}satuan-pendidikan/download?kodeKecamatan={lv3_codearea}&sortBy=bentuk_pendidikan&sortDir=asc&format=csv'
    fetch_metadata = httpx.get(metadata_url).content.decode('UTF-8')
    fetch_raw = httpx.get(data_url).content.decode('UTF-8')
    srv_timestamp = json.loads(fetch_metadata)['meta']['lastUpdatedAt']
    parse_data =  csv.DictReader(fetch_raw.splitlines())
    stacked = [dict(i, kodeKec=lv3_codearea, serverTimestamp=srv_timestamp) for i in parse_data]

    return stacked

def crawl_schlists(lv3_codeareas:list, batch_limit:int=50) -> list:
    os.makedirs('tmp', exist_ok=True)
    async def _crawl(_area:str) -> list:
        async with httpx.AsyncClient(timeout=None) as client:
            metadata_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{_area}'
            data_url = f'{TARGET_BASE_URL}satuan-pendidikan/download?kodeKecamatan={_area}&sortBy=bentuk_pendidikan&sortDir=asc&format=csv'
            fetch_metadata = await client.get(metadata_url)
            fetch_raw = await client.get(data_url)
            metadata = fetch_metadata.content.decode('UTF-8')
            resp = fetch_raw.content.decode('UTF-8')
            srv_timestamp = json.loads(metadata)['meta']['lastUpdatedAt']
            parse_data =  csv.DictReader(resp.splitlines())
            stacked = [dict(i, kodeKec=_area, serverTimestamp=srv_timestamp) for i in parse_data]
            
            return stacked
    sch_list = []
    for subset in paginator(lv3_codeareas, batch_limit):
        joblist = [_crawl(area) for area in subset]
        sequences = asyncio.run(job_aggregator(joblist))
        sch_list.append(unnest_data(sequences))
    
    repack_data = unnest_data(sch_list)

    return repack_data

def fetch_schdetail(npsn:str) -> dict:
    detail_url = f'{TARGET_BASE_URL}satuan-pendidikan/details/{npsn}'
    fetch_raw = httpx.get(detail_url).content
    raw_data = json.loads(fetch_raw)
    srv_timestamp = raw_data['meta']['lastUpdatedAt']
    data = raw_data['satuanPendidikan']
    detail_data = dict(data, serverTimestamp=srv_timestamp)

    return detail_data

def crawl_schdetail(list_npsn:list, batch_limit:int=50) -> list:
    os.makedirs('tmp', exist_ok=True)
    proc_id = randstr(10)
    async def _crawl(_npsn:str) -> dict:
        async with httpx.AsyncClient(timeout=None) as client:
            detail_url = f'{TARGET_BASE_URL}satuan-pendidikan/details/{_npsn}'
            fetch_raw = await client.get(detail_url)
            match fetch_raw.status_code:
                case 200:
                    raw_data = json.loads(fetch_raw.content)
                    srv_timestamp = raw_data['meta']['lastUpdatedAt']
                    data = raw_data['satuanPendidikan']
                    detail_data = dict(data, serverTimestamp=srv_timestamp)

                    return detail_data
                case 404:
                    detail_data = {'npsn':_npsn, 'detail_url': detail_url, 'error':'HTTP/1.1 404 Not Found' }
                    logging.warn(f"server kentod, npsn {_npsn} g onok mbut")
                    return detail_data
                case _:
                    logging.warn('unknown error')
                    pass
    
    with open(f'tmp/{proc_id}.tmp', mode='a') as f:
        for subset in paginator(list_npsn, batch_limit):
            joblist = [_crawl(npsn) for npsn in subset]
            sequences = asyncio.run(job_aggregator(joblist))
            for i in sequences:
                f.write(json.dumps(i))
                f.write('\n')
    
    with open(f'tmp/{proc_id}.tmp', mode='r') as f:
        _cache = f.read().splitlines()
        _output = [json.loads(j) for j in _cache ]
        f.close()
    os.remove(f'tmp/{proc_id}.tmp')
    
    return _output