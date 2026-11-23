// Starts tests/MqttWire.TestHost as a process of its own and stops it again. The host must have been
// built first: `dotnet build tests/MqttWire.TestHost -c Release`.

import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

/** The test host's project folder. */
const project = fileURLToPath(new URL("../MqttWire.TestHost", import.meta.url));

/** How long the host may take to print its LISTENING line, in milliseconds. */
const startTimeout = 60_000;

/** How long the host may take to stop after it was asked to, in milliseconds, before it is killed. */
const stopTimeout = 15_000;

/**
 * Starts the test host on a free port of 127.0.0.1 and waits until it listens.
 *
 * @param {string[]} [hostArguments] More arguments for the host, such as
 *   `["--max-publish-payload-bytes", "262144"]`.
 * @returns {Promise<{ url: string, stop: () => Promise<void> }>} The WebSocket URL of the host's MQTT
 *   endpoint, and `stop`, which asks the host to stop as a host does (every client is told 0x8B) and
 *   waits until its process has ended. Rejects, with the process ended, when the host does not start.
 */
export async function startHost(hostArguments = []) {
  // On POSIX the host gets a process group of its own, so that the whole of `dotnet run` can be killed.
  const child = spawn(
    "dotnet",
    [
      "run", "--project", project, "-c", "Release", "--no-build", "--no-launch-profile",
      "--", "--urls", "http://127.0.0.1:0", ...hostArguments,
    ],
    { stdio: ["pipe", "pipe", "inherit"], detached: process.platform !== "win32" },
  );
  const exited = new Promise((resolve) => {
    child.once("exit", resolve);
    child.once("error", resolve);
  });

  let port;
  try {
    port = await readPort(child, exited);
  } catch (error) {
    await kill(child, exited);
    throw error;
  }

  let stopping;
  return {
    url: `ws://127.0.0.1:${port}/mqtt`,
    stop: () => (stopping ??= stop(child, exited)),
  };
}

/**
 * Reads the host's standard output until its `LISTENING <port>` line.
 *
 * @param {import("node:child_process").ChildProcess} child The host's process.
 * @param {Promise<unknown>} exited Completes when the process has ended.
 * @returns {Promise<number>} The port. Rejects when the process ends first, or after `startTimeout`.
 */
async function readPort(child, exited) {
  const lines = createInterface({ input: child.stdout });
  const timer = AbortSignal.timeout(startTimeout);
  const listening = (async () => {
    for await (const line of lines) {
      const match = /^LISTENING (\d+)$/.exec(line);
      if (match) {
        return Number(match[1]);
      }
    }

    throw new Error("The test host ended before it listened. Was it built? (dotnet build tests/MqttWire.TestHost -c Release)");
  })();
  const timedOut = once(timer, "abort").then(() => {
    throw new Error(`The test host did not listen within ${startTimeout} ms.`);
  });
  const ended = exited.then(() => {
    throw new Error("The test host ended before it listened. Was it built? (dotnet build tests/MqttWire.TestHost -c Release)");
  });

  // The losers are still pending or reject later; neither may end the process as an unhandled rejection.
  for (const loser of [listening, timedOut, ended]) {
    loser.catch(() => {});
  }

  return await Promise.race([listening, timedOut, ended]);
}

/**
 * Asks the host to stop with a `STOP` line on its standard input, and kills it if it has not ended
 * after `stopTimeout`.
 *
 * @param {import("node:child_process").ChildProcess} child The host's process.
 * @param {Promise<unknown>} exited Completes when the process has ended.
 * @returns {Promise<void>} Completes when the process has ended.
 */
async function stop(child, exited) {
  child.stdin.on("error", () => {}); // The process may have ended already.
  child.stdin.write("STOP\n");
  let timer;
  const late = new Promise((resolve) => {
    timer = setTimeout(resolve, stopTimeout, "late");
  });
  const outcome = await Promise.race([exited, late]);
  clearTimeout(timer);
  if (outcome === "late") {
    await kill(child, exited);
  }
}

/**
 * Kills the host's process and the processes it started.
 *
 * @param {import("node:child_process").ChildProcess} child The host's process.
 * @param {Promise<unknown>} exited Completes when the process has ended.
 * @returns {Promise<void>} Completes when the process has ended.
 */
async function kill(child, exited) {
  if (child.pid === undefined) {
    return;
  }

  if (process.platform === "win32") {
    // `dotnet run` starts the host as a child of its own: /T takes the whole tree.
    const taskkill = spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
    await once(taskkill, "exit");
  } else {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      // The group has ended already.
    }
  }

  await exited;
}
