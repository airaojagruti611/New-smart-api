"""Trim only archived history that every consumer group has finished reading."""
import time


TRIM_SAFE = r"""
local function parts(id)
  local ms, seq = string.match(id, '^(%d+)%-(%d+)$')
  if not ms then error('Invalid stream ID: '..id) end
  return tonumber(ms), tonumber(seq)
end
local function earlier(a, b)
  local am, as = parts(a)
  local bm, bs = parts(b)
  return am < bm or (am == bm and as < bs)
end
if redis.call('EXISTS', KEYS[1]) == 0 then return {0, '', 'missing'} end
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
local threshold = ARGV[1]
local archive = false
local limiting = 'age'
for _, fields in ipairs(groups) do
  local group, delivered
  for i=1,#fields,2 do
    if fields[i] == 'name' then group = fields[i+1] end
    if fields[i] == 'last-delivered-id' then delivered = fields[i+1] end
  end
  if group == 'archive' then archive = true end
  if not delivered then error('Missing group checkpoint') end
  if earlier(delivered, threshold) then
    threshold = delivered
    limiting = group..':delivered'
  end
  local pending = redis.call('XPENDING', KEYS[1], group)
  if pending[1] > 0 and earlier(pending[2], threshold) then
    threshold = pending[2]
    limiting = group..':pending'
  end
end
if not archive then return {0, '', 'no_archive_group'} end
-- MINID keeps the boundary entry itself, including the oldest pending entry.
-- All reads and the trim run atomically, so new claims cannot race this check.
local removed = redis.call('XTRIM', KEYS[1], 'MINID', '~', threshold, 'LIMIT', ARGV[2])
return {removed, threshold, limiting}
"""


def trim_archived_history(client, streams, *, keep_minutes=60, now_ms=None, limit=10000):
    if keep_minutes < 30 or limit < 1:
        raise ValueError('Retain at least 30 minutes and use a positive trim limit')
    cutoff = (int(time.time() * 1000) if now_ms is None else int(now_ms)) - keep_minutes * 60000
    if cutoff <= 0:
        raise ValueError('Retention cutoff must be positive')
    trim = client.register_script(TRIM_SAFE)
    report = []
    for stream in sorted(set(streams)):
        # Candle/pivot history supports multi-day indicator warm-up.
        if stream.startswith('md:candles:') or stream == 'md:pivots:prevday':
            continue
        result = trim(keys=[stream], args=[str(cutoff) + '-0', limit])
        decode = lambda value: value.decode() if isinstance(value, bytes) else value
        report.append({'stream': stream, 'removed': int(result[0]),
                       'before_id': decode(result[1]), 'limited_by': decode(result[2])})
    return report
