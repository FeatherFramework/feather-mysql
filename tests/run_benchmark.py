"""Run the throughput/latency benchmark against a disposable local MariaDB, never server.cfg.

Mirrors run_integration.py's bootstrap; kept separate because a benchmark is something you run
occasionally by hand (`npm run bench`), not part of `npm run verify`.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[1]
server = shutil.which('mariadbd') or '/usr/sbin/mariadbd'
initializer = shutil.which('mariadb-install-db') or '/usr/bin/mariadb-install-db'
instance = Path(tempfile.mkdtemp(prefix='feather-mysql-benchmark-'))
print('Private test directory:', instance, flush=True)
process = None
try:
    with (instance / 'initialize.log').open('w') as log:
        subprocess.run([initializer, '--no-defaults', '--datadir=' + str(instance / 'data'),
                        '--auth-root-authentication-method=normal', '--skip-test-db'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    args = [server, '--no-defaults', '--datadir=' + str(instance / 'data'),
            '--socket=' + str(instance / 'mysql.sock'), '--pid-file=' + str(instance / 'mysql.pid'),
            '--log-error=' + str(instance / 'server.log'), '--skip-networking', '--skip-log-bin',
            '--innodb-buffer-pool-size=32M', '--innodb-log-file-size=16M', '--innodb-use-native-aio=0',
            '--max-connections=32']
    process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError((instance / 'server.log').read_text()[-2500:])
        if (instance / 'mysql.sock').exists():
            break
        time.sleep(0.1)
    else:
        raise RuntimeError('Private MariaDB did not become ready')
    env = dict(os.environ, FEATHER_MYSQL_PRIVATE_DIR=str(instance))
    result = subprocess.run(['node', 'tests/benchmark.js'] + sys.argv[1:], cwd=root, env=env)
    raise SystemExit(result.returncode)
finally:
    if process is not None:
        try:
            process.terminate()
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    shutil.rmtree(instance, ignore_errors=True)
    print('Private MariaDB stopped and cleaned up.', flush=True)
