
from unittest.mock import patch

import pytest, json, io, os

from converter.core import app, reset

PARENT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
FILES_DIR = os.path.join(PARENT_DIR, 'election_files')
SAMPLE_FILES = os.path.join(PARENT_DIR, 'sample_files')
SAMPLE_MAIN_FILE = os.path.join(SAMPLE_FILES, '53_5-2-2019.txt')
SAMPLE_CANDIDATE_MAPPING_FILE = os.path.join(SAMPLE_FILES, '53_CANDMAP_5-2-2019.txt')
EXPECTED_ELECTION_FILE = os.path.join(SAMPLE_FILES, '53_expected-election.json')

SAMPLE_TALLIES_FILE = os.path.join(SAMPLE_FILES, "53_tallies.json")
SAMPLE_CVRS_FILE = os.path.join(SAMPLE_FILES, "CVRs.txt")
EXPECTED_RESULTS_FILE = os.path.join(SAMPLE_FILES, '53_Results.txt')
DOUBLED_EXPECTED_RESULTS_FILE = os.path.join(SAMPLE_FILES, '53_Results_Doubled.txt')

def upload_file(client, url, filepath, extra_params={}):
    data = extra_params
    data['file'] = open(filepath,"rb")
    return client.post(
        url,
        data=data,
        content_type="multipart/form-data"
    )
    

@pytest.fixture
def client():
    reset()
    app.config['TESTING'] = True
    client = app.test_client()

    yield client

    # any cleanup goes here

def test_election_files(client):
    rv = json.loads(client.get('/convert/election/files').data)
    assert 'inputFiles' in rv
    assert 'outputFiles' in rv

def test_tallies_filelist(client):
    rv = json.loads(client.get('/convert/tallies/files').data)
    assert 'inputFiles' in rv
    assert 'outputFiles' in rv

def test_election_submitfile(client):
    upload_file(client, '/convert/election/submitfile', SAMPLE_MAIN_FILE, {
        'name': 'SEMS main file'
    })

    # check the file got there
    filelist = json.loads(client.get('/convert/election/files').data)
    assert filelist['inputFiles'][0]['name'] == 'SEMS main file'

def test_election_process(client):
    reset()
    
    # upload the sample files
    upload_file(client, '/convert/election/submitfile', SAMPLE_MAIN_FILE, {'name': 'SEMS main file'})

    # try to process before ready
    rv = client.post('/convert/election/process').data
    assert b"not all files" in rv
    
    upload_file(client, '/convert/election/submitfile', SAMPLE_CANDIDATE_MAPPING_FILE, {'name': 'SEMS candidate mapping file'})

    # try downloading before process should fail
    election_url= '/convert/election/output?name=Vx%20Election%20Definition'
    election = client.get(election_url).data
    assert election == b''
    
    # process
    client.post('/convert/election/process')

    # download and check that it's the right file
    election = json.loads(client.get(election_url).data)
    expected_election = json.loads(open(EXPECTED_ELECTION_FILE, "r").read())

    assert election == expected_election

    # request reset files
    reset_url = '/convert/reset'
    rv = client.post(reset_url).data

    # try file after reset, shouldn't be there
    rv = client.get(election_url).data
    assert rv == b""

def test_tallies_process(client):
    reset()

    upload_file(client, '/convert/tallies/submitfile', EXPECTED_ELECTION_FILE, {'name': 'Vx Election Definition'})

    rv = client.post("/convert/tallies/process").data
    assert b"not all files" in rv
    
    upload_file(client, '/convert/tallies/submitfile', SAMPLE_TALLIES_FILE, {'name': 'Vx Tallies'})

    # try file before done, shouldn't be there
    results_url = '/convert/tallies/output?name=SEMS%20Results'
    rv = client.get(results_url).data
    assert rv == b""
    
    rv = client.post("/convert/tallies/process").data
    assert json.loads(rv) == {"status": "ok"}
    
    # download and check that it's the right file
    results = client.get(results_url).data
    expected_results = open(EXPECTED_RESULTS_FILE, "rb").read()

    assert results == expected_results
    
    # request reset files
    reset_url = '/convert/reset'
    rv = client.post(reset_url).data

    # try file after reset, shouldn't be there
    rv = client.get(results_url).data
    assert rv == b""
