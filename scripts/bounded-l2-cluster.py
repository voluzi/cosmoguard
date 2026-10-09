#!/usr/bin/env python3
"""Coordinator-only guard rollout runner; see docs/bounded-l2-release-tests.md."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import threading
import time


def duration(value):
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h)", value)
    if not match:
        raise argparse.ArgumentTypeError("duration must be positive seconds, minutes or hours")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["soak", "mixed-version"])
    for name in ["context", "namespace", "new-image", "output"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--old-image", default="ghcr.io/voluzi/cosmoguard:5.1.0")
    parser.add_argument("--tools-image", default=os.environ.get("TOOLS_IMAGE"))
    parser.add_argument("--cpu", default="200m")
    parser.add_argument("--memory", default="250Mi")
    parser.add_argument("--requests-equal-limits", action="store_true")
    parser.add_argument("--duration", type=duration, default=7200)
    parser.add_argument("--rps", type=int, default=int(os.environ.get("PROBE_RPS", "0")))
    parser.add_argument("--size", type=int, default=0)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--replica-factor", type=int, choices=[1, 2, 4], default=2)
    parser.add_argument("--dmaps", type=int, choices=[4, 8])
    parser.add_argument("--restore-history", action="store_true")
    args = parser.parse_args()
    if not args.tools_image or args.rps <= 0:
        parser.error("supply --tools-image and a calibrated --rps (50% of saturation)")
    if args.mode == "soak" and not args.requests_equal_limits:
        parser.error("soaks require --requests-equal-limits")
    return args


class Run:
    def __init__(self, args):
        self.args = args
        self.root = Path(__file__).resolve().parent.parent
        self.out = Path(args.output).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.name = "l2-" + secrets.token_hex(6)
        self.owned = []
        self.children = []
        self.stop = threading.Event()
        self.sampler = None
        self.case = "setup"
        self.phase_size = args.size

    def command(self, argv, data=None):
        result = subprocess.run(argv, input=data, text=True, capture_output=True, check=True)
        return result.stdout

    def kubectl(self, *argv, data=None):
        return self.command(["kubectl", "--context", self.args.context, "-n", self.args.namespace, "--request-timeout=30s", *argv], data)

    def save(self, name, value):
        (self.out / name).write_text(value if isinstance(value, str) else json.dumps(value, indent=2))

    def create(self, resource):
        resource["metadata"].setdefault("labels", {})["bounded-l2-run"] = self.name
        resource["metadata"]["namespace"] = self.args.namespace
        result = json.loads(self.kubectl("create", "-f", "-", "-o", "json", data=json.dumps(resource)))
        identity = (result["kind"], result["metadata"]["name"], result["metadata"]["uid"])
        self.owned.append(identity)
        self.save("owned.json", self.owned)

    def image(self, ref, label):
        digest = self.command(["crane", "digest", ref]).strip()
        repository = ref.split("@")[0]
        if ":" in repository.rsplit("/", 1)[-1]:
            repository = repository.rsplit(":", 1)[0]
        pinned = repository + "@" + digest
        self.save(label + "-image.json", {"reference": ref, "digest": digest,
                  "config": json.loads(self.command(["crane", "config", pinned]))})
        return pinned

    def inventory(self, suffix):
        resources = json.loads(self.kubectl("get", "pods,services,secrets,configmaps,statefulsets,serviceaccounts,networkpolicies,poddisruptionbudgets", "-o", "json"))
        identities = [{"kind": resource["kind"], "namespace": resource["metadata"].get("namespace", ""),
                       "name": resource["metadata"]["name"], "uid": resource["metadata"]["uid"]}
                      for resource in resources["items"]]
        self.save("inventory-" + suffix + ".json", identities)

    def prepare(self):
        self.inventory("before")
        self.save("environment.json", {"arguments": vars(self.args), "run": self.name,
                  "seed": 42, "commit": self.command(["git", "-C", str(self.root), "rev-parse", "HEAD"]).strip(),
                  "kubectl": json.loads(self.command(["kubectl", "version", "--client", "-o", "json"]))})
        self.new_image = self.image(self.args.new_image, "new")
        tools = self.image(self.args.tools_image, "tools")
        if self.args.mode == "mixed-version":
            self.old_image = self.image(self.args.old_image, "old")
            published = self.command(["crane", "digest", "ghcr.io/voluzi/cosmoguard:5.1.0"]).strip()
            if not self.old_image.endswith("@" + published):
                raise RuntimeError("old image is not the published v5.1.0 manifest")
            pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": self.name + "-version"},
                   "spec": {"restartPolicy": "Never", "containers": [{"name": "version", "image": self.old_image, "args": ["--version"]}]}}
            self.create(pod)
            self.kubectl("wait", "pod/" + self.name + "-version", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=120s")
            version = self.kubectl("logs", self.name + "-version")
            self.save("old-version.txt", version)
            if "5.1.0" not in version:
                raise RuntimeError("published old image does not report 5.1.0")
        self.secret = self.name + "-secret"
        self.jwt_secret = secrets.token_hex(32)
        self.create({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": self.secret},
                     "stringData": {"CLUSTER_ENCRYPTION_KEY": base64.b64encode(secrets.token_bytes(32)).decode(),
                                    "PROBE_JWT_SECRET": self.jwt_secret}})
        self.tools = self.name + "-tools"
        self.create({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": self.tools, "labels": {"app": self.tools}},
                     "spec": {"containers": [{"name": "tools", "image": tools,
                        "envFrom": [{"secretRef": {"name": self.secret}}],
                        "securityContext": {"runAsNonRoot": True, "runAsUser": 65532}}]}})
        self.create({"apiVersion": "v1", "kind": "Service", "metadata": {"name": self.tools}, "spec": {
            "selector": {"app": self.tools}, "ports": [{"name": name, "port": port} for name, port in [("lcd", 1317), ("rpc", 26657), ("grpc", 9090)]]}})
        self.kubectl("wait", "pod/" + self.tools, "--for=condition=Ready", "--timeout=180s")

    def values(self, maps, ttl):
        ordinary = {"action": "allow", "cache": {"enable": True, "ttl": ttl},
                    "rateLimit": {"rate": "1/s", "burst": 1000, "scope": "per-identity"}}
        http_rule = dict(ordinary, paths=["/probe*"])
        sentinel = {"priority": 1, "action": "allow", "paths": ["/limit*"],
                    "rateLimit": {"rate": "1/6h", "burst": 1, "scope": "per-identity"}}
        http = {"default": "deny", "rules": [sentinel, http_rule]}
        rpc = dict(http, jsonrpc={"default": "deny", "rules": [dict(ordinary, methods=["probe"])]})
        return {"fullnameOverride": self.workload, "replicaCount": 1, "existingSecret": self.secret,
                "serviceAccount": {"create": False},
                "resources": {"limits": {"cpu": self.args.cpu, "memory": self.args.memory},
                              "requests": {"cpu": self.args.cpu, "memory": self.args.memory}},
                "config": {"enableEvm": maps == 8, "cache": {"key": self.name, "ttl": "10s", "cluster": {
                    "enable": True, "replicaCount": self.args.replica_factor, "quorum": 1}},
                    "metrics": {"enable": True}, "dashboard": {"clusterHistoryRestore": self.args.restore_history},
                    "auth": {"enable": True, "defaultRequire": True, "replayProtection": {"enable": True},
                             "methods": [{"type": "jwt", "algorithm": "HS256", "secret": "${PROBE_JWT_SECRET}"}]},
                    "nodes": [{"name": "probe", "host": self.tools, "lcdPort": 1317, "rpcPort": 26657,
                              "grpcPort": 9090, "evmRpcPort": 26657, "evmRpcWsPort": 26657,
                              "healthcheck": {"enable": False}}],
                    "lcd": http, "rpc": rpc, "grpc": {"default": "deny", "rules": [dict(ordinary, methods=["/cosmoguard.probe.Echo/Query"])]},
                    "evm": {"rpc": {"default": "deny", "rules": [dict(ordinary, methods=["probe"])], "httpRules": [http_rule]},
                            "ws": {"default": "allow", "rules": [dict(ordinary, methods=["probe"])]}}}}

    def deploy(self, maps):
        values = self.values(maps, "10s")
        self.save(self.case + "-values.json", values)
        rendered = self.command(["helm", "template", self.workload, str(self.root / "helm/cosmoguard"),
                                "--namespace", self.args.namespace, "-f", str(self.out / (self.case + "-values.json"))])
        self.save(self.case + "-rendered.yaml", rendered)
        documents = self.command(["yq", "-o=json", "-I=0", ".", "-"], rendered).splitlines()
        for line in documents:
            resource = json.loads(line)
            if resource is None:
                continue
            if resource["kind"] == "StatefulSet":
                resource["spec"]["template"]["spec"]["containers"][0]["image"] = self.new_image
            self.create(resource)
        self.kubectl("rollout", "status", "statefulset/" + self.workload, "--timeout=300s")

    def targets(self, maps):
        pods = json.loads(self.kubectl("get", "pods", "-l", "app.kubernetes.io/instance=" + self.workload, "-o", "json"))["items"]
        targets = []
        for pod in sorted(pods, key=lambda p: p["metadata"]["name"]):
            if not pod.get("status", {}).get("containerStatuses") or not all(c.get("ready") for c in pod["status"]["containerStatuses"]):
                continue
            ip = pod["status"].get("podIP")
            if not ip:
                continue
            ip = f"[{ip}]" if ":" in ip else ip
            ports = [("lcd", 11317), ("rpc", 16657), ("jsonrpc", 16657), ("grpc", 19090)]
            if maps == 8:
                ports += [("rpc", 18545), ("jsonrpc", 18545), ("jsonrpc_ws", 18546)]
            targets += [{"protocol": proto, "address": f"{ip}:{port}", "path": "/" if port == 18546 else "/websocket", "size": self.phase_size, "sentinels": port == 11317} for proto, port in ports]
        if targets:
            self.kubectl("exec", "-i", self.tools, "--", "sh", "-c", "cat > /tmp/targets-next.json && mv /tmp/targets-next.json /tmp/targets.json", data=json.dumps(targets))
        self.save(self.case + "-targets.json", targets)
        return pods

    def sample(self, maps):
        with (self.out / (self.case + "-samples.jsonl")).open("a") as stream:
            while not self.stop.is_set():
                try:
                    pods = self.targets(maps)
                    stamp = time.time_ns()
                    row = {"time_ns": stamp, "pods": pods}
                    for pod in pods:
                        name = pod["metadata"]["name"]
                        metrics = self.kubectl("get", "--raw", f"/api/v1/namespaces/{self.args.namespace}/pods/{name}:9001/proxy/metrics")
                        self.save(f"{self.case}-{stamp}-{name}.prom", metrics)
                        for line in metrics.splitlines():
                            if line.startswith("cosmoguard_l2_storage_allocated_bytes{pool=\"response\"}"):
                                allocated = float(line.split()[-1])
                                cap = next(float(x.split()[-1]) for x in metrics.splitlines() if x.startswith("cosmoguard_l2_storage_capacity_bytes{pool=\"response\"}"))
                                if cap and allocated > cap:
                                    raise RuntimeError("response cap exceeded")
                        for status in pod["status"].get("containerStatuses", []):
                            if status.get("restartCount", 0):
                                raise RuntimeError(f"unexpected restart: {name}")
                    stream.write(json.dumps(row) + "\n")
                    stream.flush()
                except subprocess.CalledProcessError as error:
                    stream.write(json.dumps({"time_ns": time.time_ns(), "sampling_error": error.stderr}) + "\n")
                except Exception as error:
                    self.save(self.case + "-invariant-error.txt", str(error))
                    self.stop.set()
                self.stop.wait(5)

    def ttl(self, value):
        cms = json.loads(self.kubectl("get", "configmaps", "-l", "app.kubernetes.io/instance=" + self.workload, "-o", "json"))["items"]
        cm = next(c for c in cms if "cosmoguard.yaml" in c.get("data", {}))
        cfg = json.loads(self.command(["yq", "-o=json", ".", "-"], cm["data"]["cosmoguard.yaml"]))
        def update(node):
            if isinstance(node, dict):
                if isinstance(node.get("cache"), dict) and node["cache"].get("enable"):
                    node["cache"]["ttl"] = value
                for child in node.values():
                    update(child)
            elif isinstance(node, list):
                for child in node:
                    update(child)
        update(cfg)
        cm["data"]["cosmoguard.yaml"] = json.dumps(cfg)
        self.kubectl("replace", "-f", "-", data=json.dumps(cm))
        self.save(self.case + "-config-" + value + ".json", cfg)

    def scale(self, replicas):
        self.kubectl("scale", "statefulset/" + self.workload, "--replicas=" + str(replicas))
        self.kubectl("rollout", "status", "statefulset/" + self.workload, "--timeout=600s")

    def switch(self, image, partition, memory):
        patch = {"spec": {"updateStrategy": {"rollingUpdate": {"partition": partition}}, "template": {"spec": {
            "containers": [{"name": "cosmoguard", "image": image, "resources": {
                "requests": {"cpu": self.args.cpu, "memory": memory}, "limits": {"cpu": self.args.cpu, "memory": memory}}}]}}}}
        self.kubectl("patch", "statefulset/" + self.workload, "--type=strategic", "-p", json.dumps(patch))
        self.kubectl("rollout", "status", "statefulset/" + self.workload, "--timeout=600s")

    def scenario(self, maps):
        self.case = f"{maps}-maps"
        self.workload = self.name + "-" + str(maps)
        self.deploy(maps)
        if self.args.mode == "mixed-version":
            self.switch(self.old_image, 0, "4Gi")
            self.scale(2)
        self.targets(maps)
        self.stop.clear()
        self.sampler = threading.Thread(target=self.sample, args=(maps,))
        self.sampler.start()
        with (self.out / (self.case + "-traffic.jsonl")).open("w") as log:
            if self.args.mode == "soak":
                steps = [lambda n=n: self.scale(n) for n in [1, 2, 4, 8, 4, 2]]
            else:
                steps = [lambda: None, lambda: self.switch(self.new_image, 2, self.args.memory),
                         lambda: self.scale(3), lambda: self.switch(self.new_image, 1, self.args.memory),
                         lambda: self.switch(self.new_image, 0, self.args.memory),
                         lambda: self.switch(self.old_image, 1, "4Gi"), lambda: self.switch(self.old_image, 0, "4Gi")]
            traffic_seconds = self.args.duration + 600 * len(steps)
            driver = subprocess.Popen(["kubectl", "--context", self.args.context, "-n", self.args.namespace,
                "exec", self.tools, "--", "l2probe", "-guard-targets", "/tmp/targets.json", "-duration",
                str(traffic_seconds) + "s", "-writers", str(self.args.workers), "-size", str(self.args.size),
                "-rps", str(self.args.rps)], stdout=log, stderr=log)
            self.children.append(driver)
            for index, step in enumerate(steps):
                if driver.poll() is not None:
                    raise RuntimeError("traffic ended before every rollout phase completed")
                if self.args.mode == "soak":
                    self.phase_size = self.args.size or [1024, 16384, 256 << 10, 1024, 16384, 256 << 10][index]
                    if index == 3:
                        self.ttl("1h")
                self.save(self.case + "-phase-" + str(index) + ".json", {"started": time.time_ns(), "size": self.phase_size, "ttl": "1h" if index >= 3 and self.args.mode == "soak" else "10s"})
                step()
                if driver.poll() is not None:
                    raise RuntimeError("traffic ended before the phase dwell completed")
                until = time.monotonic() + self.args.duration / len(steps)
                while time.monotonic() < until and driver.poll() is None and not self.stop.wait(1):
                    pass
                if self.stop.is_set():
                    raise RuntimeError("sampling invariant failed")
                if driver.poll() is not None and (index < len(steps) - 1 or time.monotonic() < until):
                    raise RuntimeError("traffic ended before every phase and dwell completed")
            if driver.wait(timeout=traffic_seconds + 120) != 0:
                raise RuntimeError("guard traffic or security sentinel failed")
        if self.args.mode == "soak":
            self.save(self.case + "-remaining-cells.json", {"status": "pending", "procedure": "docs/bounded-l2-release-tests.md"})
            # Record no-load metrics; the TTL10s expiry cell is separate.
            if self.stop.wait(900):
                raise RuntimeError("sampling invariant failed")
        self.stop.set()
        self.sampler.join()
        self.sampler = None
        for pod in self.targets(maps):
            name = pod["metadata"]["name"]
            self.save(self.case + "-" + name + ".log", self.kubectl("logs", name))
        self.scale(0)

    def cleanup(self):
        self.stop.set()
        for child in self.children:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        if self.sampler:
            self.sampler.join(timeout=60)
        failures = []
        for kind, name, uid in reversed(self.owned):
            try:
                resource = self.kubectl("get", kind, name, "--ignore-not-found", "-o", "json")
                if not resource:
                    continue
                if json.loads(resource)["metadata"]["uid"] != uid:
                    failures.append([kind, name, "uid changed; left untouched"])
                    continue
                self.kubectl("delete", kind, name, "--wait=true", "--cascade=foreground", "--timeout=120s")
            except Exception as error:
                failures.append([kind, name, str(error)])
        self.save("cleanup-errors.json", failures)
        self.inventory("after")
        if failures:
            raise RuntimeError("cleanup incomplete; inspect cleanup-errors.json")


def main():
    args = arguments()
    run = Run(args)
    for event in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(event, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        run.prepare()
        for maps in [args.dmaps] if args.dmaps else [4, 8]:
            run.scenario(maps)
        run.save("result.json", {"status": "principal traffic complete; release acceptance pending",
                                  "required_review": "docs/bounded-l2-release-tests.md"})
    finally:
        run.cleanup()


if __name__ == "__main__":
    main()
