#!/usr/bin/env python3
"""Read per-process PSS when Docker's memory cgroup accounting is unavailable."""
import json
import pathlib
import subprocess


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()


def main():
    rows = []
    for container in docker('ps', '-q', '--filter', 'label=com.docker.compose.project=taskmesh').splitlines():
        name = docker('inspect', '--format', '{{.Name}}', container).lstrip('/')
        processes = docker('top', container, '-eo', 'pid').splitlines()[1:]
        pss_kib = 0
        for pid in processes:
            path = pathlib.Path('/proc') / pid.strip() / 'smaps_rollup'
            try:
                for line in path.read_text().splitlines():
                    if line.startswith('Pss:'):
                        pss_kib += int(line.split()[1])
            except FileNotFoundError:
                pass
        rows.append({'container': name, 'pss_mib': round(pss_kib / 1024, 2), 'processes': len(processes)})
    print(json.dumps({'containers': rows, 'total_pss_mib': round(sum(row['pss_mib'] for row in rows), 2)}, indent=2))


if __name__ == '__main__':
    main()
