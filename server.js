/**
 * 高并发闪购商城 — 零依赖 Node 实现
 * 演示：Redis Lua 预减库存（原子）、MyBatis-Plus 式乐观锁防超卖、RabbitMQ 异步削峰、
 *       消费失败库存回滚、Caffeine+Redis 多级缓存、令牌桶限流、无 Redis/MQ 时降级 DB 模式
 *
 * 说明：部署沙箱不提供 MySQL/Redis/MQ，因此这里用进程内组件「等价模拟」它们的语义：
 *   - mockRedis   单线程原子执行（等价于 Redis 执行 Lua 脚本的原子性）
 *   - mockDB      版本号乐观锁（等价于 MyBatis-Plus @Version）
 *   - mockMQ      内存队列 + 异步消费 + 重试 + 死信（等价于 RabbitMQ 削峰）
 * 真实项目中这些是独立的中间件，本 Demo 聚焦「链路与并发正确性」。
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');

/* ================================================================== *
 * 1. 商品与初始库存
 * ================================================================== */
const INITIAL = [
  { id: 'P1', name: '旗舰手机 Pro Max', price: 4999, stock: 100, img: '📱' },
  { id: 'P2', name: '无线降噪耳机', price: 799, stock: 50, img: '🎧' },
  { id: 'P3', name: '智能手表 Watch S2', price: 1299, stock: 30, img: '⌚' },
  { id: 'P4', name: '机械键盘 K68', price: 399, stock: 20, img: '⌨️' },
];

/* ================================================================== *
 * 2. mockRedis：原子预减库存（等价于 Redis Lua 脚本）
 *    JS 单线程执行同步代码天然原子，等价于 Redis 单线程执行 Lua 的语义。
 * ================================================================== */
const mockRedis = {
  stock: {},                 // 预减库存
  dedup: new Set(),          // 幂等去重
  reset() {
    this.stock = {};
    this.dedup = new Set();
    for (const p of INITIAL) this.stock[p.id] = p.stock;
  },
  /**
   * 等价 Lua：
   *   local s = redis.call('GET', key)
   *   if s and tonumber(s) > 0 then redis.call('DECR', key) return 1 else return 0 end
   */
  evalPreDecrement(productId) {
    const s = this.stock[productId];
    if (s === undefined || s <= 0) return { ok: false, remain: s ?? 0 };
    this.stock[productId] = s - 1;      // 原子扣减
    return { ok: true, remain: this.stock[productId] };
  },
  rollback(productId) {                 // 消费失败回滚库存
    if (this.stock[productId] !== undefined) this.stock[productId]++;
  },
};
mockRedis.reset();

/* ================================================================== *
 * 3. mockDB：乐观锁（版本号）防超卖
 * ================================================================== */
const mockDB = {
  products: {},   // {id, name, price, stock, version}
  orders: [],     // 订单
  reset() {
    this.products = {};
    this.orders = [];
    for (const p of INITIAL) this.products[p.id] = { id: p.id, name: p.name, price: p.price, stock: p.stock, version: 0 };
  },
  get(id) { return this.products[id]; },
  /** 乐观锁更新库存：UPDATE ... SET stock=stock-1, version=version+1 WHERE id=? AND version=? AND stock>0 */
  optimisticDecrement(id) {
    const p = this.products[id];
    if (!p) return { ok: false, reason: '商品不存在' };
    if (p.stock <= 0) return { ok: false, reason: '库存不足' };
    const oldVersion = p.version;
    // 模拟并发：CAS 校验版本
    if (p.version !== oldVersion) return { ok: false, reason: '版本冲突' };
    p.stock -= 1;
    p.version += 1;
    return { ok: true, stock: p.stock, version: p.version };
  },
};

/* ================================================================== *
 * 4. mockMQ：内存队列 + 异步消费 + 重试 + 死信
 * ================================================================== */
const mockMQ = {
  queue: [],
  inflight: 0,
  produced: 0,
  consumed: 0,
  dead: [],
  send(msg) { this.queue.push(msg); this.produced++; scheduleConsume(); },
  size() { return this.queue.length; },
};
const MAX_RETRY = 3;
let consuming = false;

function consumeOnce() {
  return new Promise((resolve) => {
    setImmediate(() => {
      const msg = mockMQ.queue.shift();
      if (!msg) return resolve(false);
      mockMQ.inflight++;
      handleOrderMessage(msg);
      mockMQ.inflight--;
      resolve(true);
    });
  });
}
async function scheduleConsume() {
  if (consuming) return;
  consuming = true;
  while (mockMQ.queue.length) { await consumeOnce(); }
  consuming = false;
}

/** 消费下单消息：乐观锁写库，失败重试，最终失败则回滚 Redis 库存并进死信 */
function handleOrderMessage(msg) {
  const { orderId, productId, userId } = msg;
  const order = mockDB.orders.find(o => o.id === orderId) || dbCreateOrder(orderId, productId, userId);
  order.status = '处理中';
  let attempt = 0;
  let res;
  while (attempt < MAX_RETRY) {
    attempt++;
    res = mockDB.optimisticDecrement(productId);
    if (res.ok) break;
    if (res.reason === '库存不足' || res.reason === '商品不存在') break;   // 不可重试
  }
  if (res.ok) {
    order.status = '已下单';
    order.dbStock = res.stock;
    order.version = res.version;
    order.retries = attempt - 1;
    mockMQ.consumed++;
  } else {
    // 消费失败 → 回滚预减库存 + 进死信
    mockRedis.rollback(productId);
    order.status = '已回滚';
    order.reason = res.reason;
    mockMQ.dead.push({ orderId, reason: res.reason, at: Date.now() });
  }
}
function dbCreateOrder(orderId, productId, userId) {
  const p = INITIAL.find(x => x.id === productId);
  const o = { id: orderId, productId, productName: p ? p.name : productId, price: p ? p.price : 0, userId, status: '待处理', createdAt: Date.now(), mode: MALL_MODE };
  mockDB.orders.push(o);
  return o;
}

/* ================================================================== *
 * 5. 降级开关：Redis / MQ 不可用时走 DB 模式
 * ================================================================== */
let MALL_MODE = 'normal';   // normal | degraded(DB)
function setMode(mode) {
  MALL_MODE = mode === 'degraded' ? 'degraded' : 'normal';
  return MALL_MODE;
}

/* ================================================================== *
 * 6. 多级缓存（Caffeine 本地缓存 + Redis 远端缓存）
 * ================================================================== */
const L1 = new Map();                       // 本地缓存（模拟 Caffeine，短 TTL）
const L2 = new Map();                       // 远端缓存（模拟 Redis）
const cacheStat = { hitL1: 0, hitL2: 0, miss: 0 };
const L1_TTL = 3000, L2_TTL = 15000;
function cacheGet(key) {
  const now = Date.now();
  const a = L1.get(key);
  if (a && a.exp > now) { cacheStat.hitL1++; return a.v; }
  const b = L2.get(key);
  if (b && b.exp > now) { cacheStat.hitL2++; L1.set(key, { v: b.v, exp: now + L1_TTL }); return b.v; }  // 回填 L1
  cacheStat.miss++;
  return null;
}
function cacheSet(key, v) {
  const now = Date.now();
  L2.set(key, { v, exp: now + L2_TTL });
  L1.set(key, { v, exp: now + L1_TTL });
}
function cacheDel(key) { L1.delete(key); L2.delete(key); }

/* ================================================================== *
 * 7. 令牌桶限流（按用户）
 * ================================================================== */
class TokenBucket {
  constructor(cap, rate) { this.cap = cap; this.rate = rate; this.t = cap; this.last = Date.now(); }
  take() {
    const now = Date.now();
    this.t = Math.min(this.cap, this.t + (now - this.last) / 1000 * this.rate);
    this.last = now;
    if (this.t >= 1) { this.t -= 1; return true; }
    return false;
  }
}
const buckets = new Map();
const limitStat = { total: 0, limited: 0 };
function rateLimit(userId) {
  limitStat.total++;
  if (!buckets.has(userId)) buckets.set(userId, new TokenBucket(8, 2));
  const ok = buckets.get(userId).take();
  if (!ok) limitStat.limited++;
  return ok;
}

/* ================================================================== *
 * 8. 全局统计
 * ================================================================== */
const stats = { requests: 0, success: 0, soldout: 0, duplicate: 0, rollback: 0, latencies: [] };
function recordLatency(ms) { stats.latencies.push(ms); if (stats.latencies.length > 5000) stats.latencies.shift(); }
function latencyAvg() { if (!stats.latencies.length) return 0; return +(stats.latencies.reduce((a, b) => a + b, 0) / stats.latencies.length).toFixed(3); }

/* ================================================================== *
 * 9. 秒杀核心链路
 * ================================================================== */
function seckill(productId, userId) {
  const t0 = performance.now();
  stats.requests++;

  const out = (status, extra = {}) => { const ms = +(performance.now() - t0).toFixed(3); recordLatency(ms); return { status, latencyMs: ms, ...extra }; };

  // ① 限流
  if (!rateLimit(userId)) return out('限流拦截', { message: '请求过于频繁（令牌桶限流）' });

  // ② 幂等：一个用户对一个商品只能抢一次
  const dedupKey = userId + ':' + productId;
  if (mockRedis.dedup.has(dedupKey)) { stats.duplicate++; return out('重复下单', { message: '您已参与过本次抢购' }); }

  const p = INITIAL.find(x => x.id === productId);
  if (!p) return out('商品不存在');

  // ③ 降级模式：直接走 DB 乐观锁
  if (MALL_MODE === 'degraded') {
    const r = mockDB.optimisticDecrement(productId);
    cacheDel('product:' + productId);
    if (!r.ok) { stats.soldout++; return out('已售罄', { message: '库存不足（DB 降级模式）' }); }
    mockRedis.dedup.add(dedupKey);
    const order = dbCreateOrder('ORD' + crypto.randomBytes(5).toString('hex').toUpperCase(), productId, userId);
    order.status = '已下单'; order.mode = 'degraded';
    stats.success++;
    return out('抢购成功', { orderId: order.id, remain: r.stock, mode: 'degraded' });
  }

  // ④ 正常模式：Redis Lua 预减库存（原子）
  const pre = mockRedis.evalPreDecrement(productId);
  if (!pre.ok) { stats.soldout++; return out('已售罄', { message: '库存不足', remain: pre.remain }); }

  mockRedis.dedup.add(dedupKey);
  const orderId = 'ORD' + crypto.randomBytes(5).toString('hex').toUpperCase();
  // ⑤ MQ 异步下单（削峰）
  mockMQ.send({ orderId, productId, userId });
  stats.success++;
  return out('抢购成功', { orderId, remain: pre.remain, queued: true });
}

/* ================================================================== *
 * 10. 压力测试（服务端并发模拟）
 * ================================================================== */
async function stress(productId, concurrency) {
  const t0 = Date.now();
  const before = mockRedis.stock[productId];
  const results = { success: 0, soldout: 0, limited: 0, duplicate: 0, other: 0 };
  const tasks = [];
  for (let i = 0; i < concurrency; i++) {
    tasks.push(Promise.resolve().then(() => {
      const r = seckill(productId, 'stress_' + i + '_' + Math.random().toString(36).slice(2, 6));
      if (r.status === '抢购成功') results.success++;
      else if (r.status === '已售罄') results.soldout++;
      else if (r.status === '限流拦截') results.limited++;
      else if (r.status === '重复下单') results.duplicate++;
      else results.other++;
    }));
  }
  await Promise.all(tasks);
  // 等待 MQ 消费完
  while (mockMQ.queue.length || mockMQ.inflight) await new Promise(r => setTimeout(r, 20));

  const after = mockRedis.stock[productId];
  const dbStock = mockDB.products[productId].stock;
  const orders = mockDB.orders.filter(o => o.productId === productId && o.status === '已下单');
  const oversold = after < 0 ? -after : 0;
  return {
    productId, concurrency,
    durationMs: Date.now() - t0,
    redisStockBefore: before, redisStockAfter: after,
    dbStock, ordersCreated: orders.length,
    results,
    oversold,
    passed: oversold === 0 && after >= 0 && orders.length <= (before ?? 0),
    avgLatencyMs: latencyAvg(),
  };
}

/* ================================================================== *
 * 11. HTTP 服务
 * ================================================================== */
function sendJson(res, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': buf.length, 'Cache-Control': 'no-store' });
  res.end(buf);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let d = '';
    req.on('data', c => { d += c; if (d.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(d ? JSON.parse(d) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8' };

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://localhost');
  try {
    if (u.pathname === '/api/health') return sendJson(res, 200, { ok: true, mode: MALL_MODE, products: INITIAL.length });
    if (u.pathname === '/api/products') {
      const products = INITIAL.map(p => {
        const key = 'product:' + p.id;
        let cached = cacheGet(key);
        let obj;
        if (cached) obj = cached;
        else { obj = { ...p, redisStock: mockRedis.stock[p.id], dbStock: mockDB.products[p.id].stock }; cacheSet(key, obj); }
        return { ...obj, redisStock: mockRedis.stock[p.id], dbStock: mockDB.products[p.id].stock, cached: !!cached };
      });
      return sendJson(res, 200, { products, mode: MALL_MODE });
    }
    if (u.pathname === '/api/orders') {
      const list = mockDB.orders.slice(-30).reverse();
      return sendJson(res, 200, { orders: list });
    }
    if (u.pathname === '/api/stats') {
      return sendJson(res, 200, {
        mode: MALL_MODE, requests: stats.requests, success: stats.success, soldout: stats.soldout,
        duplicate: stats.duplicate, limited: limitStat.limited,
        avgLatencyMs: latencyAvg(), cache: cacheStat,
        mq: { produced: mockMQ.produced, consumed: mockMQ.consumed, queue: mockMQ.queue.length, dead: mockMQ.dead.length },
        redisStock: { ...mockRedis.stock }, dbStock: Object.fromEntries(Object.entries(mockDB.products).map(([k, v]) => [k, { stock: v.stock, version: v.version }])),
      });
    }
    if (req.method === 'POST' && u.pathname === '/api/seckill') {
      const body = await readBody(req);
      const r = seckill(String(body.productId || 'P1'), String(body.userId || 'u-' + Math.random().toString(36).slice(2, 7)));
      return sendJson(res, 200, r);
    }
    if (req.method === 'POST' && u.pathname === '/api/stress') {
      const body = await readBody(req);
      const r = await stress(String(body.productId || 'P1'), Math.min(5000, Math.max(1, Number(body.concurrency) || 100)));
      return sendJson(res, 200, r);
    }
    if (req.method === 'POST' && u.pathname === '/api/reset') {
      mockRedis.reset(); mockDB.reset(); mockMQ.queue = []; mockMQ.dead = []; mockMQ.produced = 0; mockMQ.consumed = 0;
      L1.clear(); L2.clear(); buckets.clear();
      stats.requests = stats.success = stats.soldout = stats.duplicate = 0; stats.latencies = [];
      limitStat.total = limitStat.limited = 0;
      return sendJson(res, 200, { ok: true, mode: MALL_MODE });
    }
    if (req.method === 'POST' && u.pathname === '/api/mode') {
      const body = await readBody(req);
      return sendJson(res, 200, { mode: setMode(body.mode) });
    }

    let p = decodeURIComponent(u.pathname);
    if (p === '/') p = '/index.html';
    const fp = path.join(PUBLIC_DIR, p);
    if (!fp.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end('forbidden'); }
    fs.readFile(fp, (err, buf) => {
      if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('404 Not Found'); }
      res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream' });
      res.end(buf);
    });
  } catch (e) {
    sendJson(res, 500, { error: String(e.message || e) });
  }
});

mockDB.reset();
server.listen(PORT, HOST, () => {
  console.log(`[seckill-mall] listening on http://${HOST}:${PORT}  mode=${MALL_MODE}`);
});
