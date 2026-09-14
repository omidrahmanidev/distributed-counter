#!/usr/bin/env python3
"""Exercise the public gateway and poll for a converged counter."""
import json
import os
import secrets
import time
import urllib.request

BASE_URL = os.environ.get('BASE_URL', 'http://localhost:8080')
video_id = secrets.randbits(52) + 1
url = f'{BASE_URL}/api/videos/{video_id}/views'


def post(email, key):
    request = urllib.request.Request(url, method='POST', headers={
        'X-User-Email': email,
        'X-Authenticated-User-Email': 'spoof@example.com',
        'Idempotency-Key': key,
    })
    with urllib.request.urlopen(request, timeout=35) as response:
        assert response.status == 202
        return json.load(response)['eventId']


first = post('User@Example.COM', 'A')
assert first == post('user@example.com', 'A'), 'Retry identity changed'
assert first != post('user@example.com', 'B'), 'Distinct view identity collapsed'
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    with urllib.request.urlopen(url, timeout=5) as response:
        count = json.load(response)['views']
    assert count <= 2, f'Duplicate counting: {count}'
    if count == 2:
        print(f'PASS: video {video_id}: three HTTP requests, two views')
        break
    time.sleep(0.25)
else:
    raise AssertionError('Counter did not converge before the deadline')
