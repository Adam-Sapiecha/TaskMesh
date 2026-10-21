#!/usr/bin/env python3
"""Authenticated HTTP smoke test; never prints credentials or other users' data."""
import argparse
import concurrent.futures
import json
import pathlib
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', default='http://127.0.0.1:8084')
    parser.add_argument('--env-file', default='.env')
    parser.add_argument('--snapshot', default='smoke-results.json')
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--jobs', type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 1000:
        parser.error('--jobs must be 1..1000')
    config = dict(line.split('=', 1) for line in pathlib.Path(args.env_file).read_text().splitlines()
                  if '=' in line and not line.lstrip().startswith('#'))
    token = config['TASKMESH_API_KEY']

    def request(method, path, body=None, key=None, auth=True):
        headers = {'Content-Type': 'application/json'}
        if auth:
            headers['Authorization'] = 'Bearer ' + token
        if key:
            headers['Idempotency-Key'] = key
        raw = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(args.base_url.rstrip('/') + path, data=raw, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    snapshot = pathlib.Path(args.snapshot)
    if args.verify_only:
        jobs = json.loads(snapshot.read_text())['job_ids']
    else:
        assert request('GET', '/api/v1/jobs', auth=False)[0] == 401
        assert request('POST', '/api/v1/jobs', {'type': 'shell.exec', 'payload': {}})[0] == 400
        body = {'type': 'report.generate', 'queue': 'reports', 'payload': {'values': [10, 20, 30]}}
        key = 'deployment-smoke-' + uuid.uuid4().hex
        status, first = request('POST', '/api/v1/jobs', body, key)
        assert status == 201, (status, first)
        status, reused = request('POST', '/api/v1/jobs', body, key)
        assert status == 200 and reused['id'] == first['id']
        changed = dict(body, payload={'values': [1, 2]})
        assert request('POST', '/api/v1/jobs', changed, key)[0] == 409

        def submit(_):
            status, job = request('POST', '/api/v1/jobs', body)
            assert status == 201, (status, job)
            return job['id']

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [first['id']] + list(pool.map(submit, range(args.jobs - 1)))
        snapshot.write_text(json.dumps({'job_ids': jobs}, indent=2) + '\n')
        snapshot.chmod(0o600)

    deadline = time.monotonic() + 120
    remaining = set(jobs)
    while remaining and time.monotonic() < deadline:
        for job_id in list(remaining):
            status, job = request('GET', '/api/v1/jobs/' + job_id)
            assert status == 200, (status, job)
            assert job['status'] not in ('DEAD_LETTER', 'CANCELLED'), job['status']
            if job['status'] == 'SUCCEEDED':
                assert job['result'] == {'count': 3, 'sum': 60, 'mean': 20, 'min': 10, 'max': 30}
                status, attempts = request('GET', '/api/v1/jobs/' + job_id + '/attempts')
                assert status == 200 and attempts[-1]['outcome'] == 'SUCCEEDED'
                remaining.remove(job_id)
        if remaining:
            time.sleep(0.25)
    assert not remaining, 'jobs did not complete before deadline'
    print(json.dumps({'status': 'passed', 'verified_jobs': len(jobs), 'base_url': args.base_url,
                      'persistence_check': args.verify_only}))


if __name__ == '__main__':
    main()
