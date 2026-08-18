/**
 * dsh-codex-bridge v0.3 — 可视化外置子代理平台
 *
 * 在 v0.2 基础上增加：
 *   - REST API: /api/agents/tasks, /api/agents/stream (SSE)
 *   - Dashboard: http://127.0.0.1:3101/agents
 *   - MCP 工具返回 dashboardUrl / taskUrl
 *   - 全局 SSE 事件推送
 */

import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, readFile, readdir, rm, stat } from 'node:fs/promises';
import { createWriteStream, accessSync, readFileSync } from 'node:fs';
import { join, resolve, basename, extname } from 'node:path';
import { homedir, hostname } from 'node:os';
import { StringDecoder } from 'node:string_decoder';
import http from 'node:http';
import { EventEmitter } from 'node:events';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { z } from 'zod';

export const name = 'dsh-codex-bridge';
export const inject = ['tools'];

// ─── 配置 ───────────────────────────────────────────────────────────────────

const TASKS_DIR = join(homedir(), '.dsh', 'tasks');
const ALLOWED_CWD = [homedir()];
const BEARER_TOKEN = process.env.DSH_CODEX_BRIDGE_TOKEN || 'dev-token-replace-in-production-1234567890';
const MCP_PORT = 3101;
const BIND_ADDR = '127.0.0.1';
const HEARTBEAT_INTERVAL = 5000;
const HEARTBEAT_TIMEOUT = 15000;
const POLL_MAX_BYTES = 64 * 1024;
const POLL_MAX_WAIT_MS = 25000;
const START_TIMEOUT_MS = 300000;
import { fileURLToPath } from 'node:url';
const DASHBOARD_PATH = join(fileURLToPath(new URL('.', import.meta.url)), 'dashboard.html');

// ─── 全局事件总线 (SSE) ──────────────────────────────────────────────────────

const taskEvents = new EventEmitter();
taskEvents.setMaxListeners(100);
let globalEventCursor = 0;

function emitTaskEvent(type, taskId, data = {}) {
  globalEventCursor++;
  const ev = { cursor: globalEventCursor, type, taskId, timestamp: Date.now(), ...data };
  taskEvents.emit('task-event', ev);
  return ev;
}

// ─── 任务状态 ────────────────────────────────────────────────────────────────

const TASK_STATUS = {
  QUEUED: 'queued', RUNNING: 'running', SUCCEEDED: 'succeeded',
  FAILED: 'failed', TIMED_OUT: 'timed_out', CANCELLED: 'cancelled', ORPHANED: 'orphaned',
};

// ─── 安全 ───────────────────────────────────────────────────────────────────

function authorize(req) {
  const auth = req.headers['authorization'];
  if (!auth || !auth.startsWith('Bearer ')) return false;
  return auth.slice(7) === BEARER_TOKEN;
}

function getDshBinJs() {
  // 返回 DSH 的真实 node 入口 (bin.js)，避免 cmd.exe 的 GBK 编码破坏中文/引号
  if (process.env.DSH_BIN_JS) return process.env.DSH_BIN_JS;
  if (process.platform === 'win32') {
    const candidate = join(process.env.APPDATA || '', 'npm', 'node_modules', '@deepseek-ai', 'dsh', 'lib', 'bin.js');
    try { accessSync(candidate); return candidate; } catch {}
  }
  return null; // 非 Windows 或找不到，supervisor 直接调用 dsh
}

// getSpawnOpts moved inline in startTask - use cmd.exe on Windows for proper stdio piping

function isAllowedCwd(dir) {
  if (!dir) return false;
  const resolved = resolve(dir).toLowerCase();
  for (const allowed of ALLOWED_CWD) {
    const a = resolve(allowed).toLowerCase();
    if (resolved === a || resolved.startsWith(a + '/') || resolved.startsWith(a + '\\')) return true;
  }
  return false;
}

function isValidTaskId(id) {
  return typeof id === 'string' && /^[a-zA-Z0-9_-]{1,128}$/.test(id);
}

// ─── TaskStore ───────────────────────────────────────────────────────────────

class TaskStore {
  constructor(baseDir) { this.baseDir = baseDir; }
  async init() { await mkdir(this.baseDir, { recursive: true }); }
  taskDir(taskId) { return join(this.baseDir, taskId); }
  async write(taskId, file, data) {
    const dir = this.taskDir(taskId); await mkdir(dir, { recursive: true });
    await writeFile(join(dir, file), JSON.stringify(data, null, 2), 'utf-8');
  }
  async writeStr(taskId, file, str) {
    const dir = this.taskDir(taskId); await mkdir(dir, { recursive: true });
    await writeFile(join(dir, file), str, 'utf-8');
  }
  async read(taskId, file) {
    try { return JSON.parse(await readFile(join(this.taskDir(taskId), file), 'utf-8')); } catch { return null; }
  }
  async readStr(taskId, file) {
    try { return await readFile(join(this.taskDir(taskId), file), 'utf-8'); } catch { return null; }
  }
  async appendEvent(taskId, event) {
    const dir = this.taskDir(taskId); await mkdir(dir, { recursive: true });
    const stream = createWriteStream(join(dir, 'events.jsonl'), { flags: 'a' });
    stream.write(JSON.stringify(event) + '\n');
    stream.end();
  }
  async listTasks() {
    try {
      const entries = await readdir(this.baseDir, { withFileTypes: true });
      const tasks = [];
      for (const e of entries) {
        if (e.isDirectory()) {
          const meta = await this.read(e.name, 'meta.json');
          if (meta) tasks.push(meta);
        }
      }
      return tasks.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    } catch { return []; }
  }
  async getTask(taskId) { return this.read(taskId, 'meta.json'); }
  async updateStatus(taskId, status, extra = {}) {
    const meta = (await this.getTask(taskId)) || {};
    meta.status = status; meta.updatedAt = Date.now();
    Object.assign(meta, extra);
    const seq = (meta.eventSeq || 0) + 1; meta.eventSeq = seq;
    await this.write(taskId, 'meta.json', meta);
    await this.appendEvent(taskId, { seq, event: 'status', status, timestamp: Date.now() });
    emitTaskEvent('task_' + status, taskId, { status, seq });
  }
  async writeHeartbeat(taskId) {
    await this.writeStr(taskId, 'heartbeat', String(Date.now()));
  }
  async readHeartbeat(taskId) {
    const s = await this.readStr(taskId, 'heartbeat');
    return s ? parseInt(s.trim(), 10) : null;
  }
  async isSupervisorAlive(taskId) {
    const meta = await this.getTask(taskId);
    if (!meta || meta.status !== TASK_STATUS.RUNNING) return false;
    const hb = await this.readHeartbeat(taskId);
    if (!hb) return false;
    return (Date.now() - hb) < HEARTBEAT_TIMEOUT;
  }
  async cleanStaleRunning() {
    const tasks = await this.listTasks();
    for (const t of tasks) {
      if (t.status === TASK_STATUS.RUNNING) {
        const alive = await this.isSupervisorAlive(t.taskId);
        if (!alive) {
          await this.updateStatus(t.taskId, TASK_STATUS.ORPHANED, { orphanedAt: Date.now() });
        }
      }
    }
  }
  async removeTask(taskId) {
    try { await rm(this.taskDir(taskId), { recursive: true, force: true }); } catch {}
  }
  async getArtifacts(taskId) {
    try {
      const dir = this.taskDir(taskId);
      const entries = await readdir(dir, { withFileTypes: true });
      const files = [];
      for (const e of entries) {
        if (e.isFile()) {
          const s = await stat(join(dir, e.name));
          files.push({ name: e.name, size: s.size, mtime: s.mtimeMs });
        }
      }
      return files;
    } catch { return []; }
  }
}

// ─── Supervisor ──────────────────────────────────────────────────────────────

class Supervisor {
  constructor(store) {
    this.store = store; this.procs = new Map(); this.hbTimers = new Map();
    this._cancelled = new Set();
  }

  async startTask(taskId, request) {
    const meta = await this.store.getTask(taskId);
    if (!meta || meta.status !== TASK_STATUS.QUEUED) {
      throw new Error(`Task ${taskId} cannot start (status=${meta?.status})`);
    }
    await this.store.updateStatus(taskId, TASK_STATUS.RUNNING, { startedAt: Date.now() });
    emitTaskEvent('task_started', taskId, { startedAt: Date.now() });

    const cwd = resolve(request.cwd || homedir());
    const timeout = request.timeoutMs || START_TIMEOUT_MS;
    const dshBinJs = getDshBinJs();
    const supervisorPath = fileURLToPath(new URL('./supervisor.mjs', import.meta.url));

    // 使用独立 supervisor 进程，避免 Windows 上 detached:true 无法捕获 stdout
    const child = spawn(process.execPath, [
      supervisorPath, taskId, this.store.baseDir, request.task,
      resolve(request.cwd || homedir()), String(timeout),
    ], {
      cwd: resolve(request.cwd || homedir()),
      shell: false,
      stdio: 'ignore',
      env: { ...process.env, ...(dshBinJs ? { DSH_BIN_JS: dshBinJs } : {}), DSH_NO_COLOR: '1', FORCE_COLOR: '0' },
      detached: true,
    });
    child.unref();
    this.procs.set(taskId, child);

    await this.store.write(taskId, 'supervisor.json', {
      taskId, pid: child.pid, dshBinJs, cwd: resolve(request.cwd || homedir()),
      spawnedAt: Date.now(), hostname: hostname(),
    });
    const m = await this.store.getTask(taskId) || {};
    m.pid = child.pid;
    await this.store.write(taskId, 'meta.json', m);

    // 监控 supervisor 退出（用于清理，不负责状态更新——supervisor 自己写文件）
    child.on('close', () => {
      this.procs.delete(taskId);
    });
  }

  async cancelTask(taskId, status = TASK_STATUS.CANCELLED) {
    this._cancelled.add(taskId);
    // 立即更新状态，让 UI 和 API 能立刻看到取消
    await this.store.write(taskId, 'result.json', { status, completedAt: Date.now() });
    await this.store.updateStatus(taskId, status, { cancelledAt: Date.now() });
    // 通过 .cancel 文件通知 supervisor 取消
    try {
      const dir = this.store.taskDir(taskId);
      await mkdir(dir, { recursive: true });
      await writeFile(join(dir, '.cancel'), '1', 'utf-8');
    } catch {}
    const t = this.hbTimers.get(taskId);
    if (t) { clearInterval(t); this.hbTimers.delete(taskId); }
    this.procs.delete(taskId);
  }

  async resumeTask(taskId) {
    const meta = await this.store.getTask(taskId);
    if (!meta || meta.status !== TASK_STATUS.RUNNING) return false;
    // 检查心跳是否仍在更新，而不是只检查 PID 是否存在
    // 桥重启后旧进程已死，但 meta.pid 仍然保留，会导致误判
    const alive = await this.store.isSupervisorAlive(taskId);
    if (!alive) {
      await this.store.updateStatus(taskId, TASK_STATUS.ORPHANED, { orphanedAt: Date.now() });
      return false;
    }
    const t = setInterval(async () => {
      const ok = await this.store.isSupervisorAlive(taskId);
      if (!ok) {
        clearInterval(t); this.hbTimers.delete(taskId);
        await this.store.updateStatus(taskId, TASK_STATUS.ORPHANED, { orphanedAt: Date.now() });
      }
    }, HEARTBEAT_INTERVAL);
    this.hbTimers.set(taskId, t);
    return true;
  }

  async resumeAll() {
    const tasks = await this.store.listTasks();
    let n = 0;
    for (const t of tasks) {
      if (t.status === TASK_STATUS.RUNNING) { try { if (await this.resumeTask(t.taskId)) n++; } catch {} }
    }
    return n;
  }

  dispose() {
    for (const t of this.hbTimers.values()) clearInterval(t);
    this.hbTimers.clear(); this.procs.clear();
  }
}

// ─── 工具函数 ────────────────────────────────────────────────────────────────

function dashboardUrl() { return `http://${BIND_ADDR}:${MCP_PORT}/agents`; }
function taskUrl(taskId) { return `http://${BIND_ADDR}:${MCP_PORT}/agents?task=${taskId}`; }

// ─── DSH 插件入口 ────────────────────────────────────────────────────────────

export function apply(ctx) {
  const logger = ctx.logger('dsh-codex-bridge');
  const store = new TaskStore(TASKS_DIR);
  const supervisor = new Supervisor(store);

  store.init()
    .then(() => store.cleanStaleRunning())
    .then(() => supervisor.resumeAll())
    .then(() => logger.info(`Task store ready: ${TASKS_DIR}`))
    .catch(e => logger.warn(`Init: ${e.message}`));

  // ─── MCP 服务器 ──────────────────────────────────────────────────────────

  const server = new McpServer({ name: 'dsh-codex-bridge', version: '0.3.0' });

  const addUrls = (obj, taskId) => ({ ...obj, dashboardUrl: dashboardUrl(), taskUrl: taskUrl(taskId) });

  server.tool('dsh_task_start', 'Start an async DSH task. Returns taskId immediately.', {
    taskId: z.string().describe('Idempotent request ID'),
    task: z.string().min(1).max(10000).describe('Task for DSH headless'),
    cwd: z.string().optional().describe('Working directory'),
    timeoutMs: z.number().min(10000).max(3600000).optional().default(300000),
  }, async (args) => {
    const { taskId } = args;
    if (!isValidTaskId(taskId)) return { content: [{ type: 'text', text: JSON.stringify({ error: 'invalid taskId' }) }], isError: true };
    const existing = await store.getTask(taskId);
    if (existing) {
      if ([TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(existing.status)) {
        return { content: [{ type: 'text', text: JSON.stringify(addUrls({ taskId, status: existing.status }, taskId)) }] };
      }
      await store.removeTask(taskId);
    }
    if (args.cwd && !isAllowedCwd(args.cwd)) {
      return { content: [{ type: 'text', text: JSON.stringify({ error: 'cwd not allowed' }) }], isError: true };
    }
    const request = { taskId, task: args.task, cwd: args.cwd || homedir(), timeoutMs: args.timeoutMs || START_TIMEOUT_MS };
    await store.write(taskId, 'request.json', request);
    await store.write(taskId, 'meta.json', { taskId, status: TASK_STATUS.QUEUED, createdAt: Date.now(), updatedAt: Date.now(), eventSeq: 0, cwd: request.cwd });
    emitTaskEvent('task_created', taskId, { task: args.task.slice(0, 200) });
    supervisor.startTask(taskId, request).catch(e => {
      logger.warn(`startTask ${taskId}: ${e.message}`);
      store.updateStatus(taskId, TASK_STATUS.FAILED, { error: e.message }).catch(() => {});
    });
    return { content: [{ type: 'text', text: JSON.stringify(addUrls({ taskId, status: TASK_STATUS.QUEUED }, taskId)) }] };
  });

  server.tool('dsh_task_poll', 'Read task events incrementally by cursor. Non-blocking, ≤64 KiB.', {
    taskId: z.string().describe('Task ID'), cursor: z.number().default(0), wait: z.boolean().default(false),
  }, async (args) => {
    const { taskId, cursor, wait } = args;
    if (!isValidTaskId(taskId)) return { content: [{ type: 'text', text: JSON.stringify({ error: 'invalid taskId' }) }], isError: true };
    const meta = await store.getTask(taskId);
    if (!meta) return { content: [{ type: 'text', text: JSON.stringify({ error: 'task not found' }) }], isError: true };
    const raw = (await store.readStr(taskId, 'events.jsonl')) || '';
    let lines = raw.split('\n').filter(Boolean);
    let events = []; let maxSeq = cursor; let bytes = 0;
    for (const line of lines) {
      try { const ev = JSON.parse(line); if (ev.seq > cursor) { const b = Buffer.byteLength(line, 'utf-8'); if (bytes + b > POLL_MAX_BYTES) break; events.push(ev); bytes += b; if (ev.seq > maxSeq) maxSeq = ev.seq; } } catch {}
    }
    if (wait && events.length === 0 && [TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(meta.status)) {
      const deadline = Date.now() + POLL_MAX_WAIT_MS;
      while (Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 500));
        const nm = await store.getTask(taskId);
        if (!nm || ![TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(nm.status)) break;
        const nr = (await store.readStr(taskId, 'events.jsonl')) || '';
        for (const line of nr.split('\n').filter(Boolean)) {
          try { const ev = JSON.parse(line); if (ev.seq > cursor) { events.push(ev); if (ev.seq > maxSeq) maxSeq = ev.seq; } } catch {}
        }
        if (events.length > 0) break;
      }
    }
    const result = await store.read(taskId, 'result.json');
    return { content: [{ type: 'text', text: JSON.stringify(addUrls({ taskId, status: meta.status, cursor: maxSeq, events, result: result || undefined }, taskId)) }] };
  });

  server.tool('dsh_task_cancel', 'Cancel a running task.', {
    taskId: z.string().describe('Task ID'),
  }, async (args) => {
    const { taskId } = args;
    if (!isValidTaskId(taskId)) return { content: [{ type: 'text', text: JSON.stringify({ error: 'invalid taskId' }) }], isError: true };
    const meta = await store.getTask(taskId);
    if (!meta) return { content: [{ type: 'text', text: JSON.stringify({ error: 'task not found' }) }], isError: true };
    if (![TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(meta.status)) {
      return { content: [{ type: 'text', text: JSON.stringify(addUrls({ taskId, status: meta.status }, taskId)) }] };
    }
    await supervisor.cancelTask(taskId);
    return { content: [{ type: 'text', text: JSON.stringify(addUrls({ taskId, status: TASK_STATUS.CANCELLED }, taskId)) }] };
  });

  server.tool('dsh_task_list', 'List all tasks with their status.', {
    filter: z.string().optional(), limit: z.number().default(50),
  }, async (args) => {
    let tasks = await store.listTasks();
    if (args.filter) tasks = tasks.filter(t => t.status === args.filter);
    tasks = tasks.slice(0, args.limit || 50);
    return { content: [{ type: 'text', text: JSON.stringify({ total: tasks.length, tasks: tasks.map(t => ({ taskId: t.taskId, status: t.status, createdAt: t.createdAt, updatedAt: t.updatedAt, exitCode: t.exitCode, taskUrl: taskUrl(t.taskId) })) }) }] };
  });

  server.tool('dsh_get_status', 'Get bridge health and stats.', {}, async () => {
    const tasks = await store.listTasks();
    const counts = {};
    for (const t of tasks) { counts[t.status] = (counts[t.status] || 0) + 1; }
    return { content: [{ type: 'text', text: JSON.stringify({ status: 'ok', version: '0.3.0', uptime: process.uptime(), taskStore: TASKS_DIR, taskCounts: counts, activeSupervisors: supervisor.procs.size, dashboardUrl: dashboardUrl() }) }] };
  });

  // ─── HTTP 服务器 ────────────────────────────────────────────────────────

  const mcpTransports = new Set();
  const sseClients = []; // dashboard SSE clients

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // ── Agents dashboard (no auth required, localhost only) ──
    if (url.pathname === '/agents' || url.pathname.startsWith('/agents/')) {
      try {
        const html = readFileSync(DASHBOARD_PATH, 'utf-8');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(html);
      } catch {
        res.writeHead(500); res.end('Dashboard not found');
      }
      return;
    }

    // ── All other endpoints require auth ──
    if (!authorize(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    // ── MCP SSE ──
    if (url.pathname === '/sse' && req.method === 'GET') {
      const t = new SSEServerTransport('/mcp', res);
      mcpTransports.add(t);
      res.on('close', () => mcpTransports.delete(t));
      server.connect(t).catch(() => {});
      return;
    }

    // ── MCP POST ──
    if (url.pathname === '/mcp' && req.method === 'POST') {
      const sid = url.searchParams.get('sessionId');
      const t = [...mcpTransports].find(x => x.sessionId === sid);
      if (t) { t.handlePostMessage(req, res).catch(() => {}); return; }
      res.writeHead(404); res.end('No session');
      return;
    }

    // ── Dashboard SSE Stream ──
    if (url.pathname === '/api/agents/stream' && req.method === 'GET') {
      const cursor = parseInt(url.searchParams.get('cursor') || '0', 10);
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
      });
      res.write('event: connected\ndata: {}\n\n');

      // Send existing tasks
      const tasks = await store.listTasks();
      res.write(`event: snapshot\ndata: ${JSON.stringify({ tasks })}\n\n`);

      const client = { res, cursor };
      sseClients.push(client);
      const keepAlive = setInterval(() => { try { res.write(':keepalive\n\n'); } catch {} }, 15000);

      const listener = (ev) => {
        if (ev.cursor > cursor) {
          try { res.write(`event: ${ev.type}\ndata: ${JSON.stringify(ev)}\n\n`); } catch {}
        }
      };
      taskEvents.on('task-event', listener);

      req.on('close', () => {
        clearInterval(keepAlive);
        taskEvents.off('task-event', listener);
        const idx = sseClients.indexOf(client);
        if (idx >= 0) sseClients.splice(idx, 1);
      });
      return;
    }

    // ── REST API: GET /api/agents/tasks ──
    if (url.pathname === '/api/agents/tasks' && req.method === 'GET') {
      const tasks = await store.listTasks();
      const filter = url.searchParams.get('filter');
      const limit = parseInt(url.searchParams.get('limit') || '50', 10);
      let result = tasks;
      if (filter) result = result.filter(t => t.status === filter);
      result = result.slice(0, limit);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ total: tasks.length, tasks: result.map(t => ({ ...t, taskUrl: taskUrl(t.taskId) })) }));
      return;
    }

    // ── REST API: POST /api/agents/tasks ──
    if (url.pathname === '/api/agents/tasks' && req.method === 'POST') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 10000) req.destroy(); });
      req.on('end', async () => {
        try {
          const data = JSON.parse(body);
          const taskId = data.taskId || 'task-' + Date.now() + '-' + randomUUID().slice(0, 8);
          if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
          const existing = await store.getTask(taskId);
          if (existing && [TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(existing.status)) {
            res.writeHead(200); res.end(JSON.stringify({ taskId, status: existing.status, taskUrl: taskUrl(taskId) }));
            return;
          }
          if (existing) await store.removeTask(taskId);
          if (data.cwd && !isAllowedCwd(data.cwd)) { res.writeHead(400); res.end(JSON.stringify({ error: 'cwd not allowed' })); return; }
          const request = { taskId, task: data.task, cwd: data.cwd || homedir(), timeoutMs: data.timeoutMs || START_TIMEOUT_MS };
          await store.write(taskId, 'request.json', request);
          await store.write(taskId, 'meta.json', { taskId, status: TASK_STATUS.QUEUED, createdAt: Date.now(), updatedAt: Date.now(), eventSeq: 0, cwd: request.cwd, model: data.model || '' });
          emitTaskEvent('task_created', taskId, { task: data.task?.slice(0, 200) });
          supervisor.startTask(taskId, request).catch(e => {
            logger.warn(`startTask ${taskId}: ${e.message}`);
            store.updateStatus(taskId, TASK_STATUS.FAILED, { error: e.message }).catch(() => {});
          });
          res.writeHead(201); res.end(JSON.stringify({ taskId, status: TASK_STATUS.QUEUED, taskUrl: taskUrl(taskId) }));
        } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); }
      });
      return;
    }

    // ── REST API: GET /api/agents/tasks/:taskId ──
    const taskMatch = url.pathname.match(/^\/api\/agents\/tasks\/([a-zA-Z0-9_-]+)$/);
    if (taskMatch && req.method === 'GET') {
      const taskId = taskMatch[1];
      if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
      const meta = await store.getTask(taskId);
      if (!meta) { res.writeHead(404); res.end(JSON.stringify({ error: 'not found' })); return; }
      const request = await store.read(taskId, 'request.json');
      const result = await store.read(taskId, 'result.json');
      const supervisorInfo = await store.read(taskId, 'supervisor.json');
      const artifacts = await store.getArtifacts(taskId);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ...meta, request, result, supervisor: supervisorInfo, artifacts, taskUrl: taskUrl(taskId) }));
      return;
    }

    // ── REST API: POST /api/agents/tasks/:taskId/cancel ──
    const cancelMatch = url.pathname.match(/^\/api\/agents\/tasks\/([a-zA-Z0-9_-]+)\/cancel$/);
    if (cancelMatch && req.method === 'POST') {
      const taskId = cancelMatch[1];
      if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
      const meta = await store.getTask(taskId);
      if (!meta) { res.writeHead(404); res.end(JSON.stringify({ error: 'not found' })); return; }
      if (![TASK_STATUS.QUEUED, TASK_STATUS.RUNNING].includes(meta.status)) {
        res.writeHead(200); res.end(JSON.stringify({ taskId, status: meta.status })); return;
      }
      await supervisor.cancelTask(taskId);
      res.writeHead(200); res.end(JSON.stringify({ taskId, status: TASK_STATUS.CANCELLED }));
      return;
    }

    // ── REST API: POST /api/agents/tasks/:taskId/retry ──
    const retryMatch = url.pathname.match(/^\/api\/agents\/tasks\/([a-zA-Z0-9_-]+)\/retry$/);
    if (retryMatch && req.method === 'POST') {
      const taskId = retryMatch[1];
      if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
      const meta = await store.getTask(taskId);
      if (!meta) { res.writeHead(404); res.end(JSON.stringify({ error: 'not found' })); return; }
      const request = await store.read(taskId, 'request.json');
      if (!request) { res.writeHead(400); res.end(JSON.stringify({ error: 'no request found' })); return; }
      const newTaskId = taskId + '-retry';
      await store.removeTask(newTaskId).catch(() => {});
      await store.write(newTaskId, 'request.json', { ...request, taskId: newTaskId });
      const newMeta = { taskId: newTaskId, status: TASK_STATUS.QUEUED, createdAt: Date.now(), updatedAt: Date.now(), eventSeq: 0, cwd: request.cwd, retryOf: taskId };
      await store.write(newTaskId, 'meta.json', newMeta);
      emitTaskEvent('task_created', newTaskId, { task: request.task?.slice(0, 200), retryOf: taskId });
      supervisor.startTask(newTaskId, request).catch(e => {
        logger.warn(`startTask ${newTaskId}: ${e.message}`);
        store.updateStatus(newTaskId, TASK_STATUS.FAILED, { error: e.message }).catch(() => {});
      });
      res.writeHead(201); res.end(JSON.stringify({ taskId: newTaskId, status: TASK_STATUS.QUEUED, taskUrl: taskUrl(newTaskId) }));
      return;
    }

    // ── REST API: GET /api/agents/tasks/:taskId/events ──
    const eventsMatch = url.pathname.match(/^\/api\/agents\/tasks\/([a-zA-Z0-9_-]+)\/events$/);
    if (eventsMatch && req.method === 'GET') {
      const taskId = eventsMatch[1];
      if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
      const cursor = parseInt(url.searchParams.get('cursor') || '0', 10);
      const meta = await store.getTask(taskId);
      if (!meta) { res.writeHead(404); res.end(JSON.stringify({ error: 'not found' })); return; }
      const raw = (await store.readStr(taskId, 'events.jsonl')) || '';
      const events = []; let maxSeq = cursor; let bytes = 0;
      for (const line of raw.split('\n').filter(Boolean)) {
        try { const ev = JSON.parse(line); if (ev.seq > cursor) { const b = Buffer.byteLength(line, 'utf-8'); if (bytes + b > POLL_MAX_BYTES) break; events.push(ev); bytes += b; if (ev.seq > maxSeq) maxSeq = ev.seq; } } catch {}
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ taskId, cursor: maxSeq, events }));
      return;
    }

    // ── REST API: GET /api/agents/tasks/:taskId/files/:filename ──
    const fileMatch = url.pathname.match(/^\/api\/agents\/tasks\/([a-zA-Z0-9_-]+)\/files\/([a-zA-Z0-9_.-]+)$/);
    if (fileMatch && req.method === 'GET') {
      const taskId = fileMatch[1];
      const filename = fileMatch[2];
      if (!isValidTaskId(taskId)) { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid taskId' })); return; }
      // Only allow known file names
      const allowed = ['request.json', 'meta.json', 'events.jsonl', 'result.json', 'heartbeat', 'supervisor.json'];
      if (!allowed.includes(filename)) { res.writeHead(403); res.end(JSON.stringify({ error: 'forbidden' })); return; }
      const filePath = join(store.taskDir(taskId), filename);
      try {
        const content = await readFile(filePath, 'utf-8');
        const ct = filename.endsWith('.json') ? 'application/json' : 'text/plain; charset=utf-8';
        res.writeHead(200, { 'Content-Type': ct });
        res.end(content);
      } catch { res.writeHead(404); res.end(JSON.stringify({ error: 'file not found' })); }
      return;
    }

    // ── Health ──
    if (url.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
      return;
    }

    // ── Root ──
    if (url.pathname === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ name: 'dsh-codex-bridge', version: '0.3.0', tools: ['dsh_task_start', 'dsh_task_poll', 'dsh_task_cancel', 'dsh_task_list', 'dsh_get_status'], dashboardUrl: dashboardUrl() }));
      return;
    }

    res.writeHead(404); res.end('Not found');
  });

  httpServer.listen(MCP_PORT, BIND_ADDR, () => {
    logger.info(`MCP server on http://${BIND_ADDR}:${MCP_PORT}`);
    logger.info(`Dashboard: http://${BIND_ADDR}:${MCP_PORT}/agents`);
    logger.info(`Tools: dsh_task_start, dsh_task_poll, dsh_task_cancel, dsh_task_list, dsh_get_status`);
  });

  ctx.on('dispose', () => {
    logger.info('Shutting down...');
    httpServer.close();
    supervisor.dispose();
    for (const t of mcpTransports) t.close();
  });
}