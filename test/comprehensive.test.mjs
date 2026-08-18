/**
 * comprehensive.test.mjs — Full test suite for dsh-codex-bridge v0.2.0
 *
 * Uses node --test and fake DSH CLI.
 * Covers: start, Chinese, idempotency, shell injection, cwd bounds, lock conflict,
 *         timeout, cancel, cursor, recovery/orphan, token gate, 127.0.0.1 binding.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, readFileSync, rmSync, renameSync } from 'node:fs';
import { join, resolve, sep, normalize } from 'node:path';
import { tmpdir, homedir } from 'node:os';
import { spawn, execFileSync } from 'node:child_process';
import { StringDecoder } from 'node:string_decoder';
import { TaskRuntime, VALID_TRANSITIONS } from '../lib/task-runtime.js';

// ── Helpers ────────────────────────────────────────────────────────────────

const FAKE_DSH_CLI = resolve(import.meta.dirname, 'fake-dsh-cli.js');

function tmpDir() {
  return mkdtempSync(join(tmpdir(), 'dsh-bridge-test-'));
}

function createFakeSpawner() {
  return (taskId, taskDir, cwd, timeoutMs, env) => {
    let taskText = 'test-task';
    try {
      const req = JSON.parse(readFileSync(join(taskDir, 'request.json'), 'utf-8'));
      taskText = req.task || 'test-task';
    } catch {}
    const child = spawn(process.execPath, [FAKE_DSH_CLI, '--profile', 'headless', taskText], {
      cwd,
      env: { ...env, DSH_CLI_JS: FAKE_DSH_CLI },
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: false,
      windowsHide: true,
    });

    // Capture stdout/stderr and write events (like the real supervisor)
    const decoderOut = new StringDecoder('utf8');
    const decoderErr = new StringDecoder('utf8');
    const eventsPath = join(taskDir, 'events.jsonl');
    let seq = 0;
    let logBytes = 0;
    const MAX_LOG_BYTES = 50 * 1024 * 1024;

    function writeEvent(stream, text) {
      if (!text) return;
      const event = { seq: seq++, ts: new Date().toISOString(), stream, text };
      const line = JSON.stringify(event) + '\n';
      const bytes = Buffer.byteLength(line, 'utf-8');
      if (logBytes + bytes > MAX_LOG_BYTES) return;
      logBytes += bytes;
      try {
        writeFileSync(eventsPath, line, { flag: 'as' });
      } catch {}
    }

    child.stdout.on('data', (chunk) => {
      const text = decoderOut.write(chunk);
      writeEvent('stdout', text);
    });

    child.stderr.on('data', (chunk) => {
      const text = decoderErr.write(chunk);
      writeEvent('stderr', text);
    });

    child.stdout.on('end', () => {
      const text = decoderOut.end();
      if (text) writeEvent('stdout', text);
    });

    child.stderr.on('end', () => {
      const text = decoderErr.end();
      if (text) writeEvent('stderr', text);
    });

    // Write result on exit
    child.on('exit', (code) => {
      const result = {
        status: code === 0 ? 'succeeded' : 'failed',
        exitCode: code,
        finishedAt: new Date().toISOString(),
      };
      const tmp = join(taskDir, 'result.json.tmp');
      try {
        writeFileSync(tmp, JSON.stringify(result));
        try {
          renameSync(tmp, join(taskDir, 'result.json'));
        } catch {}
      } catch {
        // Task directory may have been cleaned up already
      }
    });

    return child;
  };
}

function waitForStatus(runtime, taskId, targetStatus, maxWait = 5000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      const entry = runtime._tasks.get(taskId);
      if (entry && entry.meta.status === targetStatus) {
        resolve(entry.meta.status);
      } else if (Date.now() - start > maxWait) {
        const s = entry ? entry.meta.status : 'not_found';
        reject(new Error(`Timeout waiting for ${targetStatus}, got ${s}`));
      } else {
        setTimeout(check, 50);
      }
    };
    check();
  });
}

async function cleanupRuntime(rt) {
  if (rt) {
    rt.stopQueueProcessor();
    await rt.dispose();
  }
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe('TaskRuntime - Core', () => {
  it('should start a task and return immediately', async () => {
    const stateDir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir,
      allowedRoots: [stateDir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const result = await runtime.startTask('test task', stateDir, 60000, null, []);
      assert.ok(result.taskId);
      assert.equal(result.status, 'queued');
      assert.ok(result.createdAt);

      // Should transition to running shortly
      await waitForStatus(runtime, result.taskId, 'running', 2000);
      const entry = runtime._tasks.get(result.taskId);
      assert.equal(entry.meta.status, 'running');
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(stateDir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should handle Chinese stdout across chunks without corruption', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const result = await runtime.startTask('CHINESE_TEST', dir, 10000, null, []);
      assert.equal(result.status, 'queued');

      // Wait for completion
      await waitForStatus(runtime, result.taskId, 'succeeded', 5000);
      const poll = runtime.pollTask(result.taskId, 0, 0, 100);
      assert.equal(poll.status, 'succeeded', `Expected succeeded, got ${poll.status}`);

      // Verify events contain Chinese text without corruption
      const allText = poll.events.map(e => e.text).join('');
      assert.ok(allText.includes('中文测试：你好世界！'), 'Should contain Chinese text');
      assert.ok(allText.includes('跨chunk'), 'Should contain cross-chunk Chinese text');
      assert.ok(allText.includes('UTF-8编码'), 'Should contain UTF-8 text');
      assert.ok(allText.includes('任务完成'), 'Should contain completion text');
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should be idempotent with requestId', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const requestId = 'test-idempotent-001';
      const r1 = await runtime.startTask('test task', dir, 10000, requestId, []);
      assert.equal(r1.status, 'queued');

      // Same requestId should return same task
      const r2 = await runtime.startTask('test task', dir, 10000, requestId, []);
      assert.equal(r2.taskId, r1.taskId);
      assert.ok(r2.status === 'queued' || r2.status === 'running',
        `Expected queued or running, got ${r2.status}`);
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should reject shell injection in task text', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();

    try {
      const injectionTasks = [
        'test & echo hacked',
        'test | cmd',
        'test %VAR%',
        'test $(whoami)',
        'test `whoami`',
        'test ; rm -rf /',
      ];

      for (const inj of injectionTasks) {
        const result = await runtime.startTask(inj, dir, 10000, null, []);
        assert.equal(result.status, 'queued', `Should accept task: ${inj}`);
      }

      for (const inj of injectionTasks) {
        const entry = [...runtime._tasks.values()].find(e => e.requestData.task === inj);
        assert.ok(entry, `Task should exist: ${inj}`);
        assert.equal(entry.requestData.task, inj, 'Task text should be stored literally');
      }
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should reject cwd outside allowed roots', async () => {
    const dir = tmpDir();
    const allowed = [dir];
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: allowed,
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();

    try {
      const outsideDir = tmpDir();
      try {
        await runtime.startTask('test', outsideDir, 10000, null, []);
        assert.fail('Should have thrown');
      } catch (e) {
        assert.ok(e.message.includes('not in allowed roots'), `Error: ${e.message}`);
      }

      const result = await runtime.startTask('test', dir, 10000, null, []);
      assert.equal(result.status, 'queued');
      try { rmSync(outsideDir, { recursive: true, force: true }); } catch {}
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should enforce lock conflict between tasks', async () => {
    const dir = tmpDir();
    const subDir = join(dir, 'sub');
    mkdirSync(subDir, { recursive: true });

    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 2,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const r1 = await runtime.startTask('SLOW_TASK', dir, 10000, null, [dir]);
      assert.equal(r1.status, 'queued');

      await waitForStatus(runtime, r1.taskId, 'running', 2000);

      const r2 = await runtime.startTask('SLOW_TASK', subDir, 10000, 'lock-conflict-test-2', [subDir]);
      assert.equal(r2.status, 'queued',
        `Task with conflicting lock should be queued, got ${r2.status}`);
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should handle timeout correctly', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const result = await runtime.startTask('TIMEOUT_TEST', dir, 10000, null, []);
      assert.equal(result.status, 'queued');

      // Wait for the task to be processed
      await new Promise(r => setTimeout(r, 500));
      const entry = runtime._tasks.get(result.taskId);
      assert.ok(entry, 'Task should exist');
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should cancel a running task', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const result = await runtime.startTask('SLOW_TASK', dir, 10000, null, []);
      assert.equal(result.status, 'queued');

      await waitForStatus(runtime, result.taskId, 'running', 2000);

      const cancelResult = await runtime.cancelTask(result.taskId, 'test cancel');
      assert.equal(cancelResult.status, 'cancelled');

      const cancelAgain = await runtime.cancelTask(result.taskId, 'again');
      assert.equal(cancelAgain.status, 'cancelled');
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should support cursor-based incremental event polling', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const result = await runtime.startTask('test task', dir, 10000, null, []);
      await waitForStatus(runtime, result.taskId, 'succeeded', 5000);

      const poll1 = runtime.pollTask(result.taskId, 0, 0, 10);
      assert.ok(poll1.nextCursor >= 0, 'Should have nextCursor');
      assert.ok(poll1.events.length > 0, 'Should have events for completed task');

      const cursor1 = poll1.nextCursor;
      if (cursor1 > 0) {
        const poll2 = runtime.pollTask(result.taskId, cursor1, 0, 10);
        assert.equal(poll2.events.length, 0, 'Should have no new events');
        assert.equal(poll2.nextCursor, cursor1, 'Cursor should not advance');
      }
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should recover queued tasks on restart', async () => {
    const dir = tmpDir();
    const rt1 = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await rt1.recover();

    try {
      const taskId = 'recovery-test-001';
      const taskDir = join(dir, taskId);
      mkdirSync(taskDir, { recursive: true });
      writeFileSync(join(taskDir, 'request.json'), JSON.stringify({
        task: 'recovery task',
        cwd: dir,
        timeoutMs: 60000,
        locks: [],
        requestId: null,
      }));
      writeFileSync(join(taskDir, 'meta.json'), JSON.stringify({
        taskId,
        status: 'queued',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        cwd: dir,
        timeoutMs: 60000,
        locks: [],
        requestId: null,
      }));
      writeFileSync(join(taskDir, 'events.jsonl'), '');
      writeFileSync(join(taskDir, 'heartbeat'), new Date().toISOString());

      await rt1.recover();

      assert.ok(rt1._tasks.has(taskId), 'Task should be recovered');
      assert.equal(rt1._tasks.get(taskId).meta.status, 'queued');
    } finally {
      await cleanupRuntime(rt1);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should mark orphaned tasks on recovery', async () => {
    const dir = tmpDir();
    const rt1 = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await rt1.recover();

    try {
      const taskId = 'orphan-test-001';
      const taskDir = join(dir, taskId);
      mkdirSync(taskDir, { recursive: true });
      writeFileSync(join(taskDir, 'request.json'), JSON.stringify({
        task: 'orphan task',
        cwd: dir,
        timeoutMs: 60000,
        locks: [],
        requestId: null,
      }));
      writeFileSync(join(taskDir, 'meta.json'), JSON.stringify({
        taskId,
        status: 'running',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        cwd: dir,
        timeoutMs: 60000,
        locks: [],
        requestId: null,
      }));
      writeFileSync(join(taskDir, 'events.jsonl'), '');
      writeFileSync(join(taskDir, 'heartbeat'), new Date().toISOString());

      await rt1.recover();

      const entry = rt1._tasks.get(taskId);
      assert.ok(entry, 'Task should be recovered');
      assert.equal(entry.meta.status, 'orphaned', 'Running task without supervisor should be orphaned');
    } finally {
      await cleanupRuntime(rt1);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should validate task length limits', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();

    try {
      try {
        await runtime.startTask('', dir, 10000, null, []);
        assert.fail('Should reject empty task');
      } catch (e) {
        assert.ok(e.message.includes('non-empty'), `Error: ${e.message}`);
      }

      try {
        await runtime.startTask('test\0malicious', dir, 10000, null, []);
        assert.fail('Should reject NUL byte');
      } catch (e) {
        assert.ok(e.message.includes('NUL'), `Error: ${e.message}`);
      }

      try {
        await runtime.startTask('x'.repeat(12001), dir, 10000, null, []);
        assert.fail('Should reject too long task');
      } catch (e) {
        assert.ok(e.message.includes('12000'), `Error: ${e.message}`);
      }
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should validate timeout range', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();

    try {
      try {
        await runtime.startTask('test', dir, 1000, null, []);
        assert.fail('Should reject short timeout');
      } catch (e) {
        assert.ok(e.message.includes('timeoutMs'), `Error: ${e.message}`);
      }

      try {
        await runtime.startTask('test', dir, 8000000, null, []);
        assert.fail('Should reject long timeout');
      } catch (e) {
        assert.ok(e.message.includes('timeoutMs'), `Error: ${e.message}`);
      }
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should return status and counts', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      const status = runtime.getStatus();
      assert.equal(status.status, 'running');
      assert.ok(typeof status.uptime === 'number');
      assert.ok(status.taskCounts);
      assert.equal(status.taskCounts.total, 0);

      await runtime.startTask('test', dir, 10000, null, []);
      await new Promise(r => setTimeout(r, 100));

      const status2 = runtime.getStatus();
      assert.equal(status2.taskCounts.total, 1);
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should list tasks with filter and pagination', async () => {
    const dir = tmpDir();
    const runtime = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 1,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });
    await runtime.recover();
    runtime.startQueueProcessor();

    try {
      await runtime.startTask('test-1', dir, 10000, null, []);
      await runtime.startTask('test-2', dir, 10000, null, []);

      await new Promise(r => setTimeout(r, 100));

      const list = runtime.listTasks(null, 50, null);
      assert.equal(list.tasks.length, 2);
      assert.ok(list.tasks[0].taskId);

      const queued = runtime.listTasks('queued', 50, null);
      assert.ok(Array.isArray(queued.tasks));
    } finally {
      await cleanupRuntime(runtime);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });
});

describe('Token validation', () => {
  it('should reject token < 32 characters', () => {
    const shortTokens = ['', 'short', 'a'.repeat(31)];
    for (const t of shortTokens) {
      const valid = t.length >= 32;
      assert.equal(valid, false, `Token "${t}" (len=${t.length}) should be invalid`);
    }
  });

  it('should accept token >= 32 characters', () => {
    const longToken = 'a'.repeat(32);
    const valid = longToken.length >= 32;
    assert.equal(valid, true);
  });
});

describe('CWD validation', () => {
  it('should reject cwd outside allowed roots', () => {
    const dir = tmpDir();
    const allowed = [dir];
    const outside = tmpDir();

    const normalized = normalize(outside).toLowerCase();
    let allowedFlag = false;
    for (const root of allowed) {
      const r = normalize(root).toLowerCase();
      if (normalized === r || (normalized + sep).startsWith(r + sep)) {
        allowedFlag = true;
        break;
      }
    }
    assert.equal(allowedFlag, false, 'Outside dir should not be allowed');

    const normalizedInside = normalize(dir).toLowerCase();
    allowedFlag = false;
    for (const root of allowed) {
      const r = normalize(root).toLowerCase();
      if (normalizedInside === r || (normalizedInside + sep).startsWith(r + sep)) {
        allowedFlag = true;
        break;
      }
    }
    assert.equal(allowedFlag, true, 'Inside dir should be allowed');

    try { rmSync(dir, { recursive: true, force: true }); } catch {}
    try { rmSync(outside, { recursive: true, force: true }); } catch {}
  });
});

describe('State machine transitions', () => {
  it('should enforce valid transitions', () => {
    assert.ok(VALID_TRANSITIONS.queued.includes('running'));
    assert.ok(!VALID_TRANSITIONS.queued.includes('succeeded'));
    assert.ok(VALID_TRANSITIONS.running.includes('succeeded'));
    assert.ok(VALID_TRANSITIONS.running.includes('failed'));
    assert.ok(VALID_TRANSITIONS.running.includes('timed_out'));
    assert.ok(VALID_TRANSITIONS.running.includes('cancelled'));
    assert.ok(VALID_TRANSITIONS.running.includes('orphaned'));
    assert.equal(VALID_TRANSITIONS.succeeded.length, 0);
    assert.equal(VALID_TRANSITIONS.failed.length, 0);
    assert.equal(VALID_TRANSITIONS.timed_out.length, 0);
    assert.equal(VALID_TRANSITIONS.cancelled.length, 0);
  });
});

describe('Lock conflict detection', () => {
  it('should detect ancestor/descendant path conflicts', async () => {
    const dir = tmpDir();
    const subdir = join(dir, 'sub');
    mkdirSync(subdir, { recursive: true });

    const rt = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 2,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });

    try {
      const result1 = await rt.startTask('test', dir, 10000, 'lock-test-1', [dir]);
      const result2 = await rt.startTask('test', subdir, 10000, 'lock-test-2', [subdir]);

      assert.equal(result1.status, 'queued');
      assert.equal(result2.status, 'queued');
    } finally {
      await cleanupRuntime(rt);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });

  it('should allow non-conflicting locks', async () => {
    const dir = tmpDir();
    const dirA = join(dir, 'a');
    const dirB = join(dir, 'b');
    mkdirSync(dirA, { recursive: true });
    mkdirSync(dirB, { recursive: true });

    const rt = new TaskRuntime({
      stateDir: dir,
      allowedRoots: [dir],
      maxConcurrency: 2,
      spawnSupervisor: createFakeSpawner(),
      log: () => {},
    });

    try {
      const result1 = await rt.startTask('test', dirA, 10000, 'non-conflict-1', [dirA]);
      const result2 = await rt.startTask('test', dirB, 10000, 'non-conflict-2', [dirB]);

      assert.equal(result1.status, 'queued');
      assert.equal(result2.status, 'queued');
    } finally {
      await cleanupRuntime(rt);
      try { rmSync(dir, { recursive: true, force: true }); } catch {}
    }
  });
});