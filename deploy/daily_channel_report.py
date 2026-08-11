#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import collections
import datetime
import fcntl
import json
import math
import os
import random
import subprocess
import sys
import threading
import time
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
try:
    import Queue as queue
except ImportError:
    import queue


ROOT = os.environ.get('CHANNEL_ANALYSIS_ROOT', '/opt/channel-analysis')
CONFIG_PATH = os.environ.get('CHANNEL_REPORT_CONFIG', os.path.join(ROOT, 'daily.env'))
REPORT_DIR = os.environ.get('CHANNEL_REPORT_DIR', os.path.join(ROOT, 'daily'))
LOCK_PATH = os.path.join(ROOT, '.daily-report.lock')
PAGE_SIZE = 100
PAGE_DELAY_SECONDS = 0.45
LOG_PAGE_CONCURRENCY = 2
CHANNEL_REPORT_CONCURRENCY = 3
MIN_REQUEST_INTERVAL_SECONDS = 1.1
RETRY_DELAYS = [5, 15, 30, 60, 120]
ERROR_KEYWORDS = [
    u'error', u'fail', u'failed', u'timeout', u'timed out', u'超时', u'失败', u'错误',
    u'rate limit', u'rate-limit', u'quota', u'overload', u'unavailable', u'拒绝',
    u'invalid', u'connection', u'reset', u'closed', u'429', u'500', u'502', u'503',
]

try:
    text_type = unicode
except NameError:
    text_type = str


def load_config(path):
    config = {}
    with open(path, 'rb') as handle:
        for raw_line in handle:
            line = raw_line.decode('utf-8').strip()
            if not line or line.startswith(u'#') or u'=' not in line:
                continue
            key, value = line.split(u'=', 1)
            config[key.strip()] = value.strip()
    return config


def extract_items(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get('items'), list):
        return payload['items']
    data = payload.get('data')
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get('items'), list):
        return data['items']
    return []


def extract_total(payload, fallback):
    candidates = []
    if isinstance(payload, dict):
        candidates.append(payload.get('total'))
        if isinstance(payload.get('data'), dict):
            candidates.append(payload['data'].get('total'))
    for value in candidates:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            pass
    return fallback


class ApiClient(object):
    def __init__(self, base_url, api_user, cookie):
        self.base_url = base_url.rstrip('/')
        self.api_user = api_user
        self.cookie = cookie
        self.throttle_lock = threading.Lock()
        self.next_request_at = 0.0

    def wait_for_request_slot(self):
        with self.throttle_lock:
            delay = self.next_request_at - time.time()
            if delay > 0:
                time.sleep(delay)
            self.next_request_at = time.time() + MIN_REQUEST_INTERVAL_SECONDS

    def request_json(self, path, params, label):
        query = urlencode([(key, value) for key, value in params.items() if value not in (None, '')])
        url = self.base_url + path + ('?' + query if query else '')
        command = [
            '/usr/bin/curl', '-sS', '--max-time', '60',
            '-H', 'Accept: application/json',
            '-H', 'Cookie: %s' % self.cookie,
            '-H', 'New-Api-User: %s' % self.api_user,
            '-w', '\n__HTTP_STATUS__:%{http_code}',
            url,
        ]
        for attempt in range(len(RETRY_DELAYS) + 1):
            self.wait_for_request_slot()
            try:
                output = subprocess.check_output(command, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as error:
                output = error.output or b''
            marker = b'\n__HTTP_STATUS__:'
            body, separator, raw_status = output.rpartition(marker)
            try:
                status = int(raw_status.strip()) if separator else 502
            except ValueError:
                status = 502
                body = output
            if status == 200:
                return json.loads(body.decode('utf-8'))
            if status == 429 and attempt < len(RETRY_DELAYS):
                delay = RETRY_DELAYS[attempt] + random.random()
                print('%s rate limited; retrying in %.1fs' % (label, delay))
                sys.stdout.flush()
                time.sleep(delay)
                continue
            detail = body.decode('utf-8', 'replace')[:300]
            raise RuntimeError('%s failed with HTTP %s: %s' % (label, status, detail.encode('utf-8')))
        raise RuntimeError('%s failed after retries' % label)

    def fetch_all(self, path, params, label, concurrency=1):
        first_params = dict(params)
        first_params.update({'p': 1, 'page_size': PAGE_SIZE})
        first_payload = self.request_json(path, first_params, '%s page 1' % label)
        items = extract_items(first_payload)
        total = extract_total(first_payload, len(items))
        total_pages = max(1, int(math.ceil(float(total) / PAGE_SIZE))) if total else 1
        if total_pages <= 1:
            return items, total

        if concurrency <= 1:
            page = 2
            while page <= total_pages:
                time.sleep(PAGE_DELAY_SECONDS)
                page_params = dict(params)
                page_params.update({'p': page, 'page_size': PAGE_SIZE})
                payload = self.request_json(path, page_params, '%s page %s' % (label, page))
                page_items = extract_items(payload)
                items.extend(page_items)
                if not page_items:
                    break
                page += 1
            return items, total

        pending = queue.Queue()
        for page in range(2, total_pages + 1):
            pending.put(page)
        page_results = {}
        errors = []
        state_lock = threading.Lock()
        completed = [1]

        def worker():
            while not errors:
                try:
                    page = pending.get_nowait()
                except queue.Empty:
                    return
                try:
                    page_params = dict(params)
                    page_params.update({'p': page, 'page_size': PAGE_SIZE})
                    payload = self.request_json(path, page_params, '%s page %s' % (label, page))
                    with state_lock:
                        page_results[page] = extract_items(payload)
                        completed[0] += 1
                        if completed[0] % 20 == 0 or completed[0] == total_pages:
                            print('%s progress: %s/%s pages' % (label, completed[0], total_pages))
                            sys.stdout.flush()
                except Exception as error:
                    with state_lock:
                        errors.append(error)
                    return
                finally:
                    pending.task_done()
                time.sleep(PAGE_DELAY_SECONDS)

        workers = []
        for unused in range(min(concurrency, total_pages - 1)):
            thread = threading.Thread(target=worker)
            thread.daemon = True
            thread.start()
            workers.append(thread)
        for thread in workers:
            thread.join()
        if errors:
            raise errors[0]
        for page in range(2, total_pages + 1):
            items.extend(page_results.get(page, []))
        return items, total

    def fetch_first(self, path, params, label):
        page_params = dict(params)
        page_params.update({'p': 1, 'page_size': PAGE_SIZE})
        payload = self.request_json(path, page_params, label)
        items = extract_items(payload)
        return items, extract_total(payload, len(items))


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def integer(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def percentile(values, ratio):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(math.floor(len(ordered) * ratio)))
    return ordered[index]


def finite(value):
    return value is not None and not math.isnan(value) and not math.isinf(value)


def clamp(value, minimum, maximum):
    return min(maximum, max(minimum, value))


def classify(log):
    log_type = integer(log.get('type'))
    content = log.get('content') or u''
    if not isinstance(content, text_type):
        content = content.decode('utf-8', 'replace')
    lower = content.lower()
    error_signal = log_type == 5 or any(keyword in lower for keyword in ERROR_KEYWORDS)
    refund_signal = log_type == 6
    usage_signal = log_type == 2 or (not error_signal and not refund_signal and (
        log.get('quota') is not None or log.get('use_time') is not None or log.get('model_name') or log.get('model')
    ))
    return error_signal, refund_signal, usage_signal, usage_signal or error_signal


def score_success(rate):
    if rate is None:
        return 40
    if rate >= 0.98:
        return 100
    if rate >= 0.95:
        return 90
    if rate >= 0.90:
        return 78
    if rate >= 0.85:
        return 64
    if rate >= 0.75:
        return 48
    if rate >= 0.65:
        return 30
    return 15


def score_latency(average, baseline):
    if average is None:
        return 40
    if average <= 500:
        score = 100
    elif average <= 1000:
        score = 86
    elif average <= 1500:
        score = 72
    elif average <= 2500:
        score = 55
    elif average <= 4000:
        score = 35
    else:
        score = 15
    if baseline and baseline > 0:
        ratio = average / baseline
        if ratio <= 1.1:
            score += 6
        elif ratio <= 1.4:
            score -= 4
        elif ratio <= 1.8:
            score -= 10
        else:
            score -= 16
    return clamp(score, 0, 100)


def score_cost(quota_per_success, quota_per_thousand, request_count):
    if quota_per_success is None and quota_per_thousand is None:
        return 58 if request_count else 40
    if quota_per_thousand is not None:
        if quota_per_thousand <= 120:
            return 100
        if quota_per_thousand <= 200:
            return 88
        if quota_per_thousand <= 320:
            return 72
        if quota_per_thousand <= 500:
            return 55
        return 35
    if quota_per_success <= 500:
        return 100
    if quota_per_success <= 900:
        return 85
    if quota_per_success <= 1600:
        return 70
    if quota_per_success <= 3000:
        return 50
    return 28


def recommendation(score, request_count, success_rate, error_rate, average_latency, anomaly_rate, status):
    if request_count < 10:
        return u'谨慎续费', u'orange', u'样本较少，先补数据再决定是否续费。'
    if status == 0:
        return u'暂不续费', u'orange', u'渠道当前处于停用状态，不适合直接复充。'
    if success_rate < 0.8 or error_rate > 0.2 or anomaly_rate > 0.18 or (average_latency is not None and average_latency > 5000):
        return u'建议更换渠道', u'red', u'成功率或稳定性已经明显拖后腿。'
    if score >= 80 and success_rate >= 0.95 and error_rate <= 0.05 and anomaly_rate <= 0.05 and (average_latency is None or average_latency <= 1500):
        return u'建议续费', u'green', u'整体表现稳定，续费价值高。'
    if score >= 65 or (success_rate >= 0.88 and error_rate <= 0.12 and anomaly_rate <= 0.08):
        return u'谨慎续费', u'orange', u'可继续使用，但仍需控制续费节奏。'
    if anomaly_rate > 0.1 or error_rate > 0.15:
        return u'暂不续费', u'orange', u'错误或异常占比偏高，继续续费的收益有限。'
    if score < 45:
        return u'建议更换渠道', u'red', u'综合得分过低，不建议继续投入。'
    return u'暂不续费', u'orange', u'当前性价比一般，先观察再决定。'


def analyze_channel(channel, logs, totals=None):
    request_logs = []
    success_logs = []
    error_logs = []
    refund_count = 0
    for log in logs:
        error_signal, refund_signal, usage_signal, request_signal = classify(log)
        if refund_signal:
            refund_count += 1
        if request_signal:
            request_logs.append(log)
        if error_signal:
            error_logs.append(log)
        elif usage_signal:
            success_logs.append(log)

    sample_success_count = len(success_logs)
    sample_error_count = len(error_logs)
    success_count = integer(totals.get('success')) if totals else sample_success_count
    error_count = integer(totals.get('error')) if totals else sample_error_count
    refund_count = integer(totals.get('refund')) if totals else refund_count
    request_count = success_count + error_count
    success_rate = float(success_count) / request_count if request_count else None
    error_rate = float(error_count) / request_count if request_count else None
    latencies = [number(log.get('use_time')) for log in success_logs if number(log.get('use_time')) > 0]
    average_latency = sum(latencies) / len(latencies) if latencies else None
    p95_latency = percentile(latencies, 0.95)
    sample_total_quota = sum(number(log.get('quota')) for log in success_logs)
    quota_per_success = sample_total_quota / sample_success_count if sample_success_count else None
    total_quota = quota_per_success * success_count if quota_per_success is not None else 0
    token_total = sum(number(log.get('prompt_tokens')) + number(log.get('completion_tokens')) for log in success_logs)
    quota_per_thousand = sample_total_quota / token_total * 1000 if token_total else None

    model_counts = collections.Counter((log.get('model_name') or log.get('model') or u'unknown') for log in success_logs)
    entropy = 0.0
    if sample_success_count and len(model_counts) > 1:
        for count in model_counts.values():
            probability = float(count) / sample_success_count
            entropy -= probability * math.log(probability)
        entropy /= math.log(len(model_counts))

    baseline = number(channel.get('response_time'), 0.0)
    slow_threshold = max(4000.0, (p95_latency or 0) * 1.5, baseline * 3)
    anomaly_count = 0
    slow_success_sample = 0
    error_reasons = collections.Counter()
    for log in request_logs:
        error_signal = classify(log)[0]
        latency = number(log.get('use_time'))
        content = log.get('content') or u''
        if not isinstance(content, text_type):
            content = content.decode('utf-8', 'replace')
        lower = content.lower()
        slow = latency >= slow_threshold if latency > 0 else False
        if error_signal or slow:
            anomaly_count += 1
        if slow and not error_signal:
            slow_success_sample += 1
        if u'429' in lower or u'rate limit' in lower or u'限流' in lower:
            error_reasons[u'限流'] += 1
        elif u'timeout' in lower or u'超时' in lower:
            error_reasons[u'超时'] += 1
        elif any(keyword in lower for keyword in [u'connection', u'reset', u'closed', u'unavailable', u'broken']):
            error_reasons[u'连接异常'] += 1
        elif error_signal:
            error_reasons[u'其他错误'] += 1
    if totals:
        slow_rate = float(slow_success_sample) / sample_success_count if sample_success_count else 0
        estimated_slow_count = int(round(slow_rate * success_count))
        anomaly_count = min(request_count, error_count + estimated_slow_count)
    anomaly_rate = float(anomaly_count) / request_count if request_count else None

    success_score = score_success(success_rate)
    latency_score = score_latency(average_latency, baseline)
    cost_score = score_cost(quota_per_success, quota_per_thousand, request_count)
    trend_score = 65 if request_count else 40
    model_score = clamp(50 + entropy * 50, 35, 100)
    anomaly_score = clamp(100 - (anomaly_rate or 0) * 420, 12, 100) if request_count else 66
    overall_score = clamp(
        success_score * 0.35 + latency_score * 0.2 + cost_score * 0.2 +
        trend_score * 0.1 + model_score * 0.05 + anomaly_score * 0.1,
        0, 100,
    )
    label, tone, short = recommendation(
        overall_score, request_count, success_rate or 0, error_rate or 0,
        average_latency, anomaly_rate or 0, integer(channel.get('status')),
    )
    top_models = [{'model': model, 'count': count} for model, count in model_counts.most_common(5)]
    reasons = [{'reason': reason, 'count': count} for reason, count in error_reasons.most_common()]
    return {
        'id': channel.get('id'),
        'name': channel.get('name') or u'渠道 %s' % channel.get('id'),
        'status': integer(channel.get('status')),
        'score': round(overall_score, 1),
        'recommendation': label,
        'tone': tone,
        'short': short,
        'request_count': request_count,
        'success_count': success_count,
        'error_count': error_count,
        'refund_count': refund_count,
        'success_rate': round(success_rate, 6) if success_rate is not None else None,
        'error_rate': round(error_rate, 6) if error_rate is not None else None,
        'avg_latency': round(average_latency, 2) if average_latency is not None else None,
        'p95_latency': round(p95_latency, 2) if p95_latency is not None else None,
        'total_quota': round(total_quota, 2),
        'quota_per_success': round(quota_per_success, 2) if quota_per_success is not None else None,
        'anomaly_count': anomaly_count,
        'anomaly_rate': round(anomaly_rate, 6) if anomaly_rate is not None else None,
        'top_models': top_models,
        'error_reasons': reasons,
        'sample_count': len(logs),
        'is_sampled': bool(totals),
    }


def write_json_atomic(path, payload):
    temporary = path + '.tmp'
    content = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    with open(temporary, 'wb') as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.rename(temporary, path)


def main():
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR, 0o750)
    lock_handle = open(LOCK_PATH, 'w')
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print('daily report is already running', file=sys.stderr)
        return 2

    config = load_config(CONFIG_PATH)
    api_user = config.get('NEW_API_USER')
    cookie = config.get('COOKIE')
    base_url = config.get('BASE_URL', 'https://maolaoapi.com')
    if not api_user or not cookie:
        raise RuntimeError('NEW_API_USER and COOKIE are required in daily.env')

    today = datetime.date.today()
    report_date = today - datetime.timedelta(days=1)
    start_dt = datetime.datetime.combine(report_date, datetime.time.min)
    end_dt = datetime.datetime.combine(today, datetime.time.min) - datetime.timedelta(seconds=1)
    start_timestamp = int(time.mktime(start_dt.timetuple()))
    end_timestamp = int(time.mktime(end_dt.timetuple()))

    client = ApiClient(base_url, api_user, cookie)
    channels, channel_total = client.fetch_all('/api/channel/', {
        'id_sort': 'false',
        'tag_mode': 'false',
    }, 'channels')
    pending_channels = queue.Queue()
    for channel in channels:
        pending_channels.put(channel)
    results = []
    errors = []
    state_lock = threading.Lock()
    completed = [0]

    def channel_worker():
        while not errors:
            try:
                channel = pending_channels.get_nowait()
            except queue.Empty:
                return
            try:
                common_params = {
                    'channel': channel.get('id'),
                    'start_timestamp': start_timestamp,
                    'end_timestamp': end_timestamp,
                }
                usage_params = dict(common_params)
                usage_params['type'] = 2
                usage_logs, usage_total = client.fetch_first(
                    '/api/log/', usage_params, 'channel %s usage' % channel.get('id'))
                time.sleep(0.15)
                error_params = dict(common_params)
                error_params['type'] = 5
                error_logs, error_total = client.fetch_first(
                    '/api/log/', error_params, 'channel %s errors' % channel.get('id'))
                time.sleep(0.15)
                refund_params = dict(common_params)
                refund_params['type'] = 6
                refund_logs, refund_total = client.fetch_first(
                    '/api/log/', refund_params, 'channel %s refunds' % channel.get('id'))
                result = analyze_channel(channel, usage_logs + error_logs + refund_logs, {
                    'success': usage_total,
                    'error': error_total,
                    'refund': refund_total,
                })
                with state_lock:
                    results.append(result)
                    completed[0] += 1
                    if completed[0] % 5 == 0 or completed[0] == len(channels):
                        print('channel progress: %s/%s' % (completed[0], len(channels)))
                        sys.stdout.flush()
            except Exception as error:
                with state_lock:
                    errors.append(error)
                return
            finally:
                pending_channels.task_done()

    workers = []
    for unused in range(min(CHANNEL_REPORT_CONCURRENCY, len(channels))):
        thread = threading.Thread(target=channel_worker)
        thread.daemon = True
        thread.start()
        workers.append(thread)
    for thread in workers:
        thread.join()
    if errors:
        raise errors[0]

    priority = {u'建议更换渠道': 0, u'暂不续费': 1, u'谨慎续费': 2, u'建议续费': 3}
    results.sort(key=lambda item: (priority.get(item['recommendation'], 9), item['score'], item['name']))
    request_count = sum(item['request_count'] for item in results)
    success_count = sum(item['success_count'] for item in results)
    error_count = sum(item['error_count'] for item in results)
    refund_count = sum(item['refund_count'] for item in results)
    sampled_log_count = sum(item['sample_count'] for item in results)
    payload = {
        'version': 1,
        'report_date': report_date.isoformat(),
        'generated_at': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00'),
        'timezone': 'Asia/Shanghai',
        'summary': {
            'channel_count': len(results),
            'source_channel_count': channel_total,
            'source_log_count': request_count + refund_count,
            'sampled_log_count': sampled_log_count,
            'request_count': request_count,
            'success_count': success_count,
            'error_count': error_count,
            'success_rate': round(float(success_count) / request_count, 6) if request_count else None,
            'replace_count': sum(1 for item in results if item['recommendation'] == u'建议更换渠道'),
            'pause_count': sum(1 for item in results if item['recommendation'] == u'暂不续费'),
            'metric_mode': 'exact_counts_sampled_performance',
        },
        'channels': results,
    }
    archive_path = os.path.join(REPORT_DIR, report_date.isoformat() + '.json')
    latest_path = os.path.join(REPORT_DIR, 'latest.json')
    write_json_atomic(archive_path, payload)
    write_json_atomic(latest_path, payload)
    print('daily report generated: %s channels, %s sampled logs, %s requests' % (
        len(results), sampled_log_count, request_count))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('daily report failed: %s' % error, file=sys.stderr)
        sys.exit(1)
