#!/usr/bin/env python3
"""Copy completed backend filesystems; invoked by the dataset Makefiles."""

import csv
import fcntl
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid


def log(message):
    print(message, flush=True)


def capture(command):
    return subprocess.check_output(command, text=True).strip()


def inspect(kind, name):
    result = subprocess.run(
        ["docker", kind, "inspect", name], capture_output=True, text=True
    )
    if result.returncode:
        if "no such" in result.stderr.lower():
            return None
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout)[0]


def run_live(label, command, env=None):
    started = time.monotonic()
    process = subprocess.Popen(command, env=env)
    try:
        while True:
            try:
                code = process.wait(timeout=10)
                break
            except subprocess.TimeoutExpired:
                log(f"{label}: still running ({time.monotonic() - started:.0f}s elapsed)")
        if code:
            raise RuntimeError(f"{label} failed (exit {code})")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def mount_arg(**fields):
    # Docker's --mount value is CSV, including when host paths contain commas.
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="").writerow(
        f"{key}={value}" for key, value in fields.items()
    )
    return buffer.getvalue()


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def source_details(backend, compose_project, compose_config):
    container = inspect("container", f"{compose_project}-{backend}")
    volume_name = f"{compose_project}_{backend}_data"
    volume = inspect("volume", volume_name)
    if volume is None:
        raise RuntimeError(f"{backend}: source volume is missing: {volume_name}")
    relative = PurePosixPath(".")
    if container:
        labels = container["Config"].get("Labels") or {}
        if (labels.get("com.docker.compose.project") != compose_project or
                labels.get("com.docker.compose.service") != backend):
            raise RuntimeError(f"{backend}: source container belongs to another project")
        environment = dict(item.split("=", 1) for item in container["Config"]["Env"])
        data_path = PurePosixPath(
            "/usr/share/elasticsearch/data" if backend == "elasticsearch"
            else environment["PGDATA"]
        )
        mounts = [mount for mount in container["Mounts"]
                  if data_path.is_relative_to(mount["Destination"])]
        if not mounts:
            raise RuntimeError(f"{backend}: no persistent mount covers {data_path}")
        mount = max(mounts, key=lambda item: len(item["Destination"]))
        if mount["Type"] != "volume":
            raise RuntimeError(f"{backend}: expected a Docker data volume")
        # In the original plain-PG configuration the real PGDATA is inside
        # the anonymous parent volume, not the empty named child volume.
        volume_name = mount["Name"]
        volume = inspect("volume", volume_name)
        relative = data_path.relative_to(mount["Destination"])
        image = container["Image"]
    else:
        image = compose_config["services"][backend]["image"]
    if inspect("image", image) is None:
        raise RuntimeError(f"{backend}: the source image is not installed: {image}")
    options = volume.get("Options") or {}
    host_path = Path(options.get("device", volume["Mountpoint"])) / str(relative)
    return dict(container=container, volume=volume_name, relative=str(relative),
                image=image, host_path=str(host_path.resolve()))


COPY_SCRIPT = r'''
set -eu
source_dir="/source/$1"
target_dir="/target/$2"
backend=$3
if [ "$backend" = elasticsearch ]; then
    test -d "$source_dir/indices" || {
        echo 'Source is not an initialized Elasticsearch data directory' >&2; exit 1;
    }
else
    test -s "$source_dir/PG_VERSION" && test -d "$source_dir/base" || {
        echo 'Source is not an initialized PostgreSQL data directory' >&2; exit 1;
    }
    if [ -L "$source_dir/pg_wal" ] ||
       [ -n "$(find "$source_dir/pg_tblspc" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo 'External PostgreSQL WAL or tablespaces require a separate copy plan' >&2; exit 1
    fi
fi
cp -a --reflink=never --sparse=auto "$source_dir/." "$target_dir/"
sync -f "$target_dir"
'''


def copy_files(backend, source, destination):
    temporary = Path(tempfile.mkdtemp(prefix=f".{backend}.copy-", dir=destination.parent))
    worker = "bench-copy-" + uuid.uuid4().hex
    target_mount = mount_arg(type="bind", source=destination.parent, target="/target")
    try:
        run_live(f"{backend}: copying files", [
            "docker", "run", "--rm", "--name", worker, "--network", "none",
            "--read-only", "--user", "0", "--entrypoint", "/bin/sh",
            "--mount", mount_arg(type="volume", source=source["volume"],
                                 target="/source", readonly="true"),
            "--mount", target_mount, source["image"], "-c", COPY_SCRIPT,
            "copy-files", source["relative"], temporary.name, backend,
        ])
        if destination.exists():
            destination.rmdir()  # Only a pre-existing empty directory is allowed.
        temporary.rename(destination)
    finally:
        # Killing the CLI alone does not necessarily stop its Docker container.
        subprocess.run(["docker", "rm", "-f", worker], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        if temporary.exists():
            subprocess.run([
                "docker", "run", "--rm", "--network", "none", "--read-only",
                "--user", "0", "--entrypoint", "/bin/rm", "--mount", target_mount,
                source["image"], "-rf", "--", f"/target/{temporary.name}",
            ], check=True)


def main():
    env = os.environ
    backends = env["BACKENDS"].replace(",", " ").split()
    source_state = Path(env["STORAGE_STATE_DIR"])
    target_state = Path(env["TARGET_STATE_DIR"])
    target_parent = Path(env["TARGET_VOLUME_ROOT"]) / env["PROJECT"]
    source_compose = ["docker", "compose", "--project-name", env["COMPOSE_PROJECT"],
                      "-f", str(Path(env["CONFIG_DIR"]) / "compose.yml")]
    if env["VOLUME_ROOT"]:
        source_compose += ["-f", str(Path(env["CONFIG_DIR"]) / "compose.volumes.yml")]
    config = json.loads(capture([*source_compose, "config", "--format", "json"]))
    target_env = dict(env, VOLUME_ROOT=env["TARGET_VOLUME_ROOT"],
                      COMPOSE_PROJECT=env["TARGET_COMPOSE_PROJECT"],
                      STORAGE_STATE_DIR=str(target_state))
    target_compose = ["docker", "compose", "--project-name", env["TARGET_COMPOSE_PROJECT"],
                      "-f", str(Path(env["CONFIG_DIR"]) / "compose.yml"),
                      "-f", str(Path(env["CONFIG_DIR"]) / "compose.volumes.yml")]
    subprocess.run([*target_compose, "config", "--quiet"], env=target_env, check=True)
    plans = []
    for backend in backends:
        done = source_state / f"{backend}.done"
        if not done.is_file():
            raise RuntimeError(f"{backend}: source has not completed setup at this VOLUME_ROOT")
        source = source_details(backend, env["COMPOSE_PROJECT"], config)
        destination = target_parent / backend
        common = os.path.commonpath([source["host_path"], str(destination.resolve())])
        if common in (source["host_path"], str(destination.resolve())):
            raise RuntimeError(f"{backend}: source and target directories overlap")
        identity = dict(source_volume=source["volume"], source_subdirectory=source["relative"],
                        source_done=done.read_text(), destination=str(destination))
        receipt_path = target_state / f"{backend}.copy.json"
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
        resume = (receipt.get("files_copied") and receipt.get("source") == identity
                  and destination.is_dir() and not (target_state / f"{backend}.done").exists())
        if not resume:
            for suffix in ("loaded", "done"):
                if (target_state / f"{backend}.{suffix}").exists():
                    raise RuntimeError(f"{backend}: target already has setup records")
            if destination.exists() or destination.is_symlink():
                try:
                    occupied = (destination.is_symlink() or not destination.is_dir()
                                or any(destination.iterdir()))
                except PermissionError:
                    occupied = True
                if occupied:
                    raise RuntimeError(f"{backend}: target already contains data: {destination}")
            if (inspect("volume", f"{env['TARGET_COMPOSE_PROJECT']}_{backend}_data") or
                    inspect("container", f"{env['TARGET_COMPOSE_PROJECT']}-{backend}")):
                raise RuntimeError(f"{backend}: target already has a Docker volume or container")
        else:
            target_volume = inspect("volume", f"{env['TARGET_COMPOSE_PROJECT']}_{backend}_data")
            if target_volume and (target_volume["Driver"] != "local" or target_volume.get("Options") != {
                "type": "none", "o": "bind", "device": str(destination)
            }):
                raise RuntimeError(f"{backend}: target volume has conflicting storage settings")
        plans.append((backend, source, destination, identity, receipt_path, resume))

    # All selected backends must pass preflight before any source is stopped.
    target_parent.mkdir(parents=True, exist_ok=True)
    with (target_parent / ".copy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        running = [source["container"]["Id"] for _, source, _, _, _, resume in plans
                   if not resume and source["container"] and source["container"]["State"]["Running"]]
        if running:
            log("Stopping source databases cleanly before copying")
            run_live("Stopping source databases", ["docker", "stop", "--time", "-1", *running])
        target_state.mkdir(parents=True, exist_ok=True)
        for backend, source, destination, identity, receipt_path, resume in plans:
            started = time.monotonic()
            if resume:
                log(f"{backend}: files already copied; completing Docker registration")
            else:
                active = capture(["docker", "ps", "--filter", f"volume={source['volume']}",
                                  "--format", "{{.Names}}"])
                if active:
                    raise RuntimeError(f"{backend}: source volume is still in use by: {active}")
                log(f"{backend}: {source['host_path']} -> {destination}")
                copy_files(backend, source, destination)
                atomic_json(receipt_path, dict(source=identity, files_copied=True,
                                              image=source["image"]))
            run_live(f"{backend}: registering copied database", [
                *target_compose, "create", "--no-build", "--pull", "never", backend,
            ], env=target_env)
            # Publish completion last; an interrupted copy cannot be benchmarked.
            for suffix in ("loaded", "done"):
                marker = source_state / f"{backend}.{suffix}"
                if marker.exists():
                    temporary = target_state / f"{backend}.{suffix}.tmp"
                    shutil.copy2(marker, temporary)
                    temporary.replace(target_state / f"{backend}.{suffix}")
            log(f"{backend}: copy complete ({(time.monotonic() - started) * 1000:.0f} ms)")
    log(f"Copy complete. Run with VOLUME_ROOT={env['TARGET_VOLUME_ROOT']} BACKENDS={env['BACKENDS']}")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        main()
    except KeyboardInterrupt:
        log("Copy interrupted; source data retained")
        sys.exit(130)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"copy: {error}", file=sys.stderr)
        sys.exit(1)
