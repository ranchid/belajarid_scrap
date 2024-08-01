import csv
import json
import asyncio
import httpx

from httpx import ReadTimeout, RemoteProtocolError

TARGET_BASE_URL = 'https://api.data.belajar.id/data-portal-backend/v1/master-data/'

MAIN_AREA = '360'

def parse_subarea(decoded_content:str, parent:str) -> list:
    area_list = []
    for area in json.loads(decoded_content)['data']:
        data = area['district']
        data['kodeIndukWilayah'] = parent
        area_list.append(data)

    return area_list

def unnest_data(nested_data:list) -> list:
    flatten_data = [datum for array_datum in nested_data for datum in array_datum]
    return flatten_data


def paginator(items:list, items_per_page:int):
    pages = [items[item:item+items_per_page] for item in range(0, len(items), items_per_page)]
    return pages

async def job_aggregator(job_list):
    job_agg = await asyncio.gather(*job_list)
    return job_agg

def fetch_subarea(parent_area:str) -> list:
    parent_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{parent_area}/descendants?sortBy=bentuk_pendidikan&sortDir=asc'
    fetch_raw = httpx.get(parent_url).content.decode('UTF-8')
    data = parse_subarea(fetch_raw, parent_area)
    
    return data

def crawl_subareas(parent_area:list) -> list:    
    async def _crawl(_area:str) -> dict:
        async with httpx.AsyncClient(timeout=None) as client:
            parent_url = f'{TARGET_BASE_URL}satuan-pendidikan/statistics/{_area}/descendants?sortBy=bentuk_pendidikan&sortDir=asc'
            fetch_raw = await client.get(parent_url)
            resp = fetch_raw.content.decode('UTF-8')
            parsed_data = parse_subarea(resp,_area)
            
            return parsed_data
    joblist = [_crawl(area) for area in parent_area]
    grab_data = asyncio.run(job_aggregator(joblist))

    repack_data = unnest_data(grab_data)
    
    return repack_data

def fetch_schlist(lv3_codearea:str) -> list:
    data_url = f'{TARGET_BASE_URL}satuan-pendidikan/download?kodeKecamatan={lv3_codearea}&sortBy=bentuk_pendidikan&sortDir=asc&format=csv'
    fetch_raw = httpx.get(data_url).content.decode('UTF-8')
    parse_data =  csv.DictReader(fetch_raw.splitlines())
    stacked = [i for i in parse_data]

    return stacked

def crawl_schlists(lv3_codeareas:list, batch_limit:int=50) -> list:
    async def _crawl(_area:str) -> list:
        async with httpx.AsyncClient(timeout=None) as client:
            data_url = f'{TARGET_BASE_URL}satuan-pendidikan/download?kodeKecamatan={_area}&sortBy=bentuk_pendidikan&sortDir=asc&format=csv'
            fetch_raw = await client.get(data_url)
            resp = fetch_raw.content.decode('UTF-8')
            parse_data =  csv.DictReader(resp.splitlines())
            stacked = [i for i in parse_data]
            
            return stacked
    sch_list = []
    for subset in paginator(lv3_codeareas, batch_limit):
        joblist = [_crawl(area) for area in subset]
        sequences = asyncio.run(job_aggregator(joblist))
        sch_list.append(unnest_data(sequences))
    
    repack_data = unnest_data(sch_list)

    return repack_data