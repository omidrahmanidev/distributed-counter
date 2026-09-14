import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
export const options = {
  vus: Number(__ENV.VUS || 20), duration: __ENV.DURATION || '30s',
  thresholds: { http_req_failed: ['rate<0.01'], checks: ['rate==1'] },
};
const base = __ENV.BASE_URL || 'http://localhost:8080';
const video = __ENV.VIDEO_ID || '123';
const mode = __ENV.MODE || 'new';
export function setup() {
  const before = http.get(`${base}/api/videos/${video}/views`).json('views');
  return { before, run: `${Date.now()}-${Math.random().toString(36).slice(2)}` };
}
export default function(data) {
  // Retry mode shares one operation across all VUs; its expected delta is exactly one.
  const key = mode === 'retry' ? `retry-${data.run}` : `${data.run}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
  const result = http.post(`${base}/api/videos/${video}/views`, null, {
    headers: { 'X-User-Email': 'load@example.com', 'Idempotency-Key': key },
  });
  check(result, { 'Kafka accepted': r => r.status === 202 });
}
export function teardown(data) {
  if (mode !== 'retry') return;
  const deadline = Date.now() + Number(__ENV.CONVERGENCE_SECONDS || 60) * 1000;
  let count = data.before;
  while (Date.now() < deadline) {
    count = http.get(`${base}/api/videos/${video}/views`).json('views');
    if (count >= data.before + 1) break;
    sleep(1);
  }
  check(count, { 'retry operation counted exactly once': n => n === data.before + 1 });
}
