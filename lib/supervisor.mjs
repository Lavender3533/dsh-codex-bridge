/**
 * supervisor.mjs — 独立 DSH 子代理 Supervisor 进程
 *
 * 由 dsh-codex-bridge 以 detached 模式启动。
 * 负责：
 *   1. 以 shell:true 启动 DSH headless 并捕获 stdout/stderr
 *   2. 写入事件到 events.jsonl
 *   3. 定期写 heartbeat
 *   4. 完成时写 result.json
 *   5. 监听 cancel 信号（通过文件）
 */

import { spawn } from 'node:child_process';
import { mkdir, writeFile, appendFile, readFile, unlink } from 'node:fs/promises';
import { createWriteStream, existsSync } from 'node:fs';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import { hostname } from 'node:os';

const [taskId, tasksDir, taskStr, cwd, timeoutMs] = process.argv.slice(2);
const dshBin = process.env.DSH_PATH || 'dsh';

if (!taskId || !tasksDir || !taskStr) {
  console.error('Usage: supervisor.mjs <taskId> <tasksDir> <task> [cwd] [timeoutMs]');
  process.exit(1);
}

const TASK_DIR = join(tasksDir, taskId);
const HEARTBEAT_INTERVAL = 5000;
const TIMEOUT = parseInt(timeoutMs || '300000', 10);
const WORK_DIR = cwd || process.env.HOME || process.env.USERPROFILE;

// ─── 辅助函数 ──────────────────────────────────────────────────────────────

async function readJSON(file) {
  try { return JSON.parse(await readFile(join(TASK_DIR, file), 'utf-8')); } catch { return null; }
}

async function writeJSON(file, data) {
  await mkdir(TASK_DIR, { recursive: true });
  await writeFile(join(TASK_DIR, file), JSON.stringify(data, null, 2), 'utf-8');
}

async function appendEvent(event) {
  await mkdir(TASK_DIR, { recursive: true });
  const stream = createWriteStream(join(TASK_DIR, 'events.jsonl'), { flags: 'a' });
  stream.write(JSON.stringify(event) + '\n');
  stream.end();
}

async function writeHeartbeat() {
  await mkdir(TASK_DIR, { recursive: true });
  await writeFile(join(TASK_DIR, 'heartbeat'), String(Date.now()), 'utf-8');
}

async function checkCancelSignal() {
  try {
    const data = await readFile(join(TASK_DIR, '.cancel'), 'utf-8');
    if (data.trim() === '1') {
      await unlink(join(TASK_DIR, '.cancel')).catch(() => {});
      return true;
    }
  } catch {}
  return false;
}

// ─── 主流程 ─────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  // 更新状态为 running
  await writeJSON('meta.json', {
    taskId, status: 'running', startedAt: Date.now(), updatedAt: Date.now(), eventSeq: 1, pid: process.pid,
  });
  await appendEvent({ seq: 1, event: 'status', status: 'running', timestamp: Date.now() });

  // 启动 DSH headless
  const child = spawn(dshBin, ['--profile', 'headless', taskStr], {
    cwd: WORK_DIR,
    shell: true,  // Windows 需要 shell:true 来运行 .cmd 文件
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, DSH_NO_COLOR: '1', FORCE_COLOR: '0' },
  });

  const pid = child.pid;

  // 写入 supervisor 信息
  await writeJSON('supervisor.json', {
    taskId, pid, dshBin, cwd: WORK_DIR, spawnedAt: Date.now(), hostname: hostname(),
  });

  // 心跳
  const hbTimer = setInterval(writeHeartbeat, HEARTBEAT_INTERVAL);

  // 解码器
  const outDec = new StringDecoder('utf-8');
  const errDec = new StringDecoder('utf-8');
  let seq = 1;

  // 取消信号检查
  const cancelTimer = setInterval(async () => {
    if (await checkCancelSignal()) {
      clearInterval(cancelTimer);
      clearInterval(hbTimer);
      try { process.kill(-child.pid, 'SIGTERM'); } catch {}
      try { process.kill(child.pid, 'SIGTERM'); } catch {}
      await writeJSON('result.json', { status: 'cancelled', completedAt: Date.now() });
      await appendEvent({ seq: ++seq, event: 'status', status: 'cancelled', timestamp: Date.now() });
      const meta = await readJSON('meta.json') || {};
      meta.status = 'cancelled'; meta.updatedAt = Date.now(); meta.completedAt = Date.now();
      await writeJSON('meta.json', meta);
      await writeHeartbeat();
      process.exit(0);
    }
  }, 1000);

  // 超时
  const timeoutTimer = setTimeout(async () => {
    clearInterval(cancelTimer);
    clearInterval(hbTimer);
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    try { process.kill(child.pid, 'SIGTERM'); } catch {}
await writeJSON('result.json', { status: 'timed_out', completedAt: Date.now() });
      await appendEvent({ seq: ++seq, event: 'status', status: 'timed_out', timestamp: Date.now() });
      const meta = await readJSON('meta.json') || {};
      meta.status = 'timed_out'; meta.updatedAt = Date.now(); meta.completedAt = Date.now();
      await writeJSON('meta.json', meta);
      await writeHeartbeat();
    process.exit(0);
  }, TIMEOUT);

  child.stdout.on('data', (chunk) => {
    const text = outDec.write(chunk);
    seq++;
    appendEvent({ seq, event: 'stdout', stream: 'stdout', text, cursor: Buffer.byteLength(text), timestamp: Date.now() }).catch(() => {});
  });

  child.stderr.on('data', (chunk) => {
    const text = errDec.write(chunk);
    seq++;
    appendEvent({ seq, event: 'stderr', stream: 'stderr', text, cursor: Buffer.byteLength(text), timestamp: Date.now() }).catch(() => {});
  });

  child.on('close', async (code) => {
    clearInterval(cancelTimer);
    clearTimeout(timeoutTimer);
    clearInterval(hbTimer);
    outDec.end();
    errDec.end();

    const finalStatus = code === 0 ? 'succeeded' : 'failed';
    const result = { status: finalStatus, exitCode: code, completedAt: Date.now(), duration: Date.now() - startTime };
    await writeJSON('result.json', result);
    await appendEvent({ seq: ++seq, event: 'status', status: finalStatus, timestamp: Date.now() });
    // 更新 meta.json 状态
    const meta = await readJSON('meta.json') || {};
    meta.status = finalStatus; meta.updatedAt = Date.now(); meta.exitCode = code; meta.completedAt = Date.now();
    await writeJSON('meta.json', meta);
    await writeHeartbeat();
    process.exit(0);
  });

  child.on('error', async (err) => {
    clearInterval(cancelTimer);
    clearTimeout(timeoutTimer);
    clearInterval(hbTimer);
    await writeJSON('result.json', { status: 'failed', error: err.message, completedAt: Date.now() });
    await appendEvent({ seq: ++seq, event: 'status', status: 'failed', timestamp: Date.now() });
    const meta = await readJSON('meta.json') || {};
    meta.status = 'failed'; meta.updatedAt = Date.now(); meta.error = err.message;
    await writeJSON('meta.json', meta);
    await writeHeartbeat();
    process.exit(1);
  });
}

main().catch(async (err) => {
  await writeJSON('result.json', { status: 'failed', error: err.message, completedAt: Date.now() });
  const meta = await readJSON('meta.json') || {};
  meta.status = 'failed'; meta.updatedAt = Date.now(); meta.error = err.message;
  await writeJSON('meta.json', meta);
  process.exit(1);
});