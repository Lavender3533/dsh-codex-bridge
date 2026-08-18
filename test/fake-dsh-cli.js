#!/usr/bin/env node
/**
 * Fake DSH CLI for testing — simulates DSH behavior without real DSH.
 * Usage: node fake-dsh-cli.js --profile headless "task text"
 *
 * Behaviors:
 *   - Normal: prints some output and exits 0
 *   - Slow: sleeps then exits
 *   - Chinese: prints Chinese text across chunks
 *   - Timeout: sleeps longer than timeout
 *   - Shell injection: just echoes args
 */

// ESM compatible (package.json has "type": "module")

const task = process.argv.slice(2).join(' ');

if (task.includes('SHELL_INJECTION_TEST')) {
  process.stdout.write('ARGS: ' + process.argv.slice(2).join('|') + '\n');
  process.exit(0);
}

if (task.includes('CHINESE_TEST')) {
  // Write Chinese text across multiple chunks
  const text1 = '中文测试：你好世界！\n';
  const text2 = '这是跨chunk的中文文本测试。\n';
  const text3 = 'UTF-8编码确保不乱码。\n';
  process.stdout.write(text1);
  const buf = Buffer.from(text2, 'utf-8');
  const split = Math.floor(buf.length / 2);
  process.stdout.write(buf.slice(0, split));
  setTimeout(() => {
    process.stdout.write(buf.slice(split));
    process.stdout.write(text3);
    setTimeout(() => {
      process.stdout.write('任务完成。\n');
      process.exit(0);
    }, 50);
  }, 50);
} else if (task.includes('SLOW_TASK')) {
  setTimeout(() => {
    process.stdout.write('Slow task completed\n');
    process.exit(0);
  }, 300);
} else if (task.includes('TIMEOUT_TEST')) {
  // Sleep longer than timeout
  setTimeout(() => {
    process.stdout.write('Should not reach here\n');
    process.exit(0);
  }, 5000);
} else if (task.includes('FAIL_TASK')) {
  process.stderr.write('Task failed intentionally\n');
  process.exit(1);
} else {
  // Default: normal output
  process.stdout.write('Executing task: ' + task + '\n');
  process.stdout.write('Status: OK\n');
  process.exit(0);
}