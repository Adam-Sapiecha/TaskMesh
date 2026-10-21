import { useEffect, useRef, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  ArrowDownLeft,
  ArrowRight,
  Boxes,
  Check,
  CheckCircle2,
  ChevronRight,
  Clock3,
  Code2,
  Cpu,
  FileText,
  Hash,
  Layers3,
  LoaderCircle,
  Play,
  RefreshCw,
  Server,
  ShieldCheck,
  Terminal,
  X,
  XCircle,
} from "lucide-react";
import "./style.css";

type Job = {
  id: string;
  type: string;
  queue: string;
  payload: Record<string, unknown>;
  status: string;
  priority: number;
  attempt_count: number;
  max_attempts: number;
  lease_generation: number;
  created_at: string;
  completed_at: string | null;
  result: Record<string, unknown> | null;
  last_error: string | null;
};
type Worker = {
  name: string;
  concurrency: number;
  active: number;
  online: boolean;
  heartbeat_at: string;
};
type Session = { submitted: number; expires_at: string };
type Attempt = {
  generation: number;
  outcome: string | null;
  started_at: string;
  finished_at: string | null;
  error: string | null;
};
type Event = {
  id: number;
  status: string;
  created_at: string;
  lease_generation: number;
};
const apiBase = new URL("./public/v1/", window.location.href).pathname;
const portfolio = "https://adam-sapiecha.github.io/Portfolio-AdamSapiecha/";
const types = [
  {
    id: "text.analyze",
    name: "Text analysis",
    caption: "Words, characters & lines",
    icon: FileText,
    example:
      "Distributed work. Durable results.\nTaskMesh runs background jobs on independent Go workers.",
  },
  {
    id: "report.generate",
    name: "Numeric report",
    caption: "Sum, mean & distribution",
    icon: Layers3,
    example: "12, 28, 19, 45, 31, 62, 24, 38",
  },
  {
    id: "benchmark.compute",
    name: "SHA-256 compute",
    caption: "Repeated cryptographic hashing",
    icon: Hash,
    example: "A small input. A real computation.",
  },
];
const terminal = new Set(["SUCCEEDED", "DEAD_LETTER", "CANCELLED"]);
const statusLabel = (s: string) =>
  ({
    PENDING: "Queued",
    RUNNING: "Running",
    SUCCEEDED: "Completed",
    RETRY_SCHEDULED: "Retry scheduled",
    DEAD_LETTER: "Failed",
    CANCELLED: "Cancelled",
  })[s] || s;
const time = (s: string) =>
  new Date(s).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
const duration = (j: Job) =>
  j.completed_at
    ? `${Math.max(0, new Date(j.completed_at).getTime() - new Date(j.created_at).getTime())} ms`
    : "—";
async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(apiBase + path, {
    ...options,
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  const value = await response.json();
  if (!response.ok)
    throw new Error(value.error || "Service unavailable. Please try again.");
  return value as T;
}
function Mark() {
  return (
    <span className="mark" aria-hidden="true">
      <Boxes size={24} strokeWidth={1.6} />
    </span>
  );
}
function App() {
  const [session, setSession] = useState<Session | null>(null),
    [jobs, setJobs] = useState<Job[]>([]),
    [workers, setWorkers] = useState<Worker[]>([]),
    [error, setError] = useState(""),
    [ready, setReady] = useState(false),
    [sending, setSending] = useState(false),
    [selected, setSelected] = useState(types[0]),
    [input, setInput] = useState(types[0].example),
    [count, setCount] = useState(1),
    [priority, setPriority] = useState(5),
    [detail, setDetail] = useState<Job | null>(null),
    [attempts, setAttempts] = useState<Attempt[]>([]),
    [events, setEvents] = useState<Event[]>([]),
    [filter, setFilter] = useState("all"),
    [tab, setTab] = useState("jobs");
  const booted = useRef(false),
    selectedID = useRef<string | null>(null),
    working = useRef(false);
  const refresh = async () => {
    if (working.current) return;
    working.current = true;
    try {
      const [nextJobs, nextWorkers] = await Promise.all([
        api<Job[]>("jobs"),
        api<Worker[]>("workers"),
      ]);
      setJobs(nextJobs);
      setWorkers(nextWorkers);
      setReady(true);
      if (selectedID.current) {
        const found = nextJobs.find((j) => j.id === selectedID.current);
        if (found) {
          setDetail(found);
          const [a, e] = await Promise.all([
            api<Attempt[]>(`jobs/${found.id}/attempts`),
            api<Event[]>(`jobs/${found.id}/events`),
          ]);
          setAttempts(a);
          setEvents(e);
        }
      }
    } catch (e) {
      setReady(false);
      setError((e as Error).message);
    } finally {
      working.current = false;
    }
  };
  useEffect(() => {
    if (booted.current) return;
    booted.current = true;
    let disposed = false;
    const start = async () => {
      try {
        const s = await api<Session>("session", { method: "POST", body: "{}" });
        if (!disposed) {
          setSession(s);
          await refresh();
        }
      } catch (e) {
        if (!disposed) setError((e as Error).message);
      }
    };
    void start();
    const timer = window.setInterval(() => {
      if (booted.current && document.visibilityState === "visible")
        void refresh();
    }, 2500);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, []);
  const choose = (id: string) => {
    const next = types.find((t) => t.id === id)!;
    setSelected(next);
    setInput(next.example);
    setError("");
  };
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError("");
    setSending(true);
    try {
      let payload: Record<string, unknown>;
      if (selected.id === "text.analyze") {
        if (!input.trim()) throw new Error("Enter some text to analyze.");
        payload = { text: input };
      } else if (selected.id === "report.generate") {
        const values = input
          .split(/[\s,;]+/)
          .filter(Boolean)
          .map(Number);
        if (!values.length || values.some((v) => !Number.isFinite(v)))
          throw new Error("Enter numbers separated by commas or spaces.");
        payload = { values };
      } else payload = { seed: input, rounds: 10000 };
      const runID = crypto.randomUUID();
      for (let i = 0; i < count; i++)
        await api<Job>("jobs", {
          method: "POST",
          headers: { "Idempotency-Key": `${runID}-${i}` },
          body: JSON.stringify({
            type: selected.id,
            queue: selected.id === "report.generate" ? "reports" : "default",
            priority,
            max_attempts: 3,
            payload,
          }),
        });
      setSession(await api<Session>("session", { method: "POST", body: "{}" }));
      await refresh();
      setTab("jobs");
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSending(false);
    }
  };
  const inspect = async (job: Job) => {
    selectedID.current = job.id;
    setDetail(job);
    setAttempts([]);
    setEvents([]);
    try {
      const [a, e] = await Promise.all([
        api<Attempt[]>(`jobs/${job.id}/attempts`),
        api<Event[]>(`jobs/${job.id}/events`),
      ]);
      setAttempts(a);
      setEvents(e);
    } catch (e) {
      setError((e as Error).message);
    }
  };
  const completed = jobs.filter((j) => j.status === "SUCCEEDED").length,
    active = jobs.filter((j) => j.status === "RUNNING").length,
    queued = jobs.filter((j) =>
      ["PENDING", "RETRY_SCHEDULED"].includes(j.status),
    ).length;
  const shown = jobs.filter(
    (j) =>
      filter === "all" ||
      (filter === "completed"
        ? j.status === "SUCCEEDED"
        : !terminal.has(j.status)),
  );
  const close = () => {
    selectedID.current = null;
    setDetail(null);
  };
  useEffect(() => {
    if (!detail) return;
    const fn = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("keydown", fn);
    return () => window.removeEventListener("keydown", fn);
  }, [detail]);
  return (
    <div className="app">
      <a className="skip" href="#main">
        Skip to console
      </a>
      <aside className="sidebar">
        <a className="brand" href="./">
          <Mark />
          <span>
            taskmesh<span className="brand-dot">.</span>
          </span>
        </a>
        <div className="workspace">
          <span className="workspace-icon">AS</span>
          <div>
            <strong>Portfolio cluster</strong>
            <small>Adam Sapiecha</small>
          </div>
          <span className="tiny-tag">LIVE</span>
        </div>
        <p className="nav-label">WORKSPACE</p>
        <nav>
          <button
            className="nav-item selected"
            onClick={() => {
              setTab("jobs");
              document.querySelector("#main")?.scrollIntoView();
            }}
          >
            <Activity size={18} />
            Execution console<span className="nav-shortcut">01</span>
          </button>
          <button
            className="nav-item"
            onClick={() => {
              setTab("workers");
              document
                .querySelector("#activity")
                ?.scrollIntoView({ behavior: "smooth" });
            }}
          >
            <Server size={18} />
            Worker fleet
            <span className="nav-number">
              {workers.filter((w) => w.online).length}
            </span>
          </button>
          <button
            className="nav-item"
            onClick={() =>
              document
                .querySelector("#guarantees")
                ?.scrollIntoView({ behavior: "smooth" })
            }
          >
            <ShieldCheck size={18} />
            How it works
          </button>
        </nav>
        <div className="sidebar-bottom">
          <div className="runtime-note">
            <span className="green-dot" />
            <span>Running on a Raspberry Pi</span>
            <small>Go · PostgreSQL · Linux ARM64</small>
          </div>
          <a className="back-link" href={portfolio}>
            <ArrowDownLeft size={16} />
            Back to portfolio
          </a>
        </div>
      </aside>
      <div className="content">
        <header className="topbar">
          <div className="breadcrumb">
            <span>Workspace</span>
            <ChevronRight size={13} />
            <strong>Execution console</strong>
          </div>
          <div className="topbar-right">
            <span className="connection">
              <span className={ready ? "green-dot" : "amber-dot"} />
              {ready ? "System operational" : "Connecting"}
            </span>
            <span className="avatar">AS</span>
          </div>
        </header>
        <main id="main">
          <div className="heading-row">
            <div>
              <div className="eyebrow">
                <span className="green-dot" />
                DISTRIBUTED TASK EXECUTION
              </div>
              <h1>
                Small tasks. <span>Built to go further.</span>
              </h1>
              <p className="lead">
                Submit a job. Follow its journey. Inspect the result.
                <br />
                Real workers, a durable queue, and your own space to explore.
              </p>
            </div>
            <div className="environment">
              <Terminal size={15} />
              <span>portfolio / production</span>
              <span className="environment-dot" />
            </div>
          </div>
          <section className="stats" aria-label="Your session statistics">
            {[
              {
                title: "Jobs submitted",
                value: jobs.length,
                icon: Boxes,
                foot: "In your current session",
              },
              {
                title: "In progress",
                value: active,
                icon: Activity,
                foot: `${queued} waiting in the queue`,
              },
              {
                title: "Completed",
                value: completed,
                icon: CheckCircle2,
                foot: jobs.length
                  ? `${Math.round((completed / jobs.length) * 100)}% completion rate`
                  : "Ready when you are",
              },
              {
                title: "Workers online",
                value: `${workers.filter((w) => w.online).length} / 3`,
                icon: Cpu,
                foot: "Independent Go processes",
              },
            ].map((s) => (
              <article className="stat" key={s.title}>
                <div>
                  <span>{s.title}</span>
                  <s.icon size={17} />
                </div>
                <strong>{s.value}</strong>
                <small>{s.foot}</small>
              </article>
            ))}
          </section>
          {error && (
            <div className="error" role="alert">
              <XCircle size={17} />
              <span>{error}</span>
              <button aria-label="Dismiss error" onClick={() => setError("")}>
                <X size={16} />
              </button>
            </div>
          )}
          <div className="work-grid">
            <section className="panel composer">
              <div className="panel-heading">
                <div className="section-icon">
                  <Play size={16} />
                </div>
                <div>
                  <h2>Run something real</h2>
                  <p>Your input. Our queue. A result you can inspect.</p>
                </div>
                <span className="subtle-tag">NEW JOB</span>
              </div>
              <form onSubmit={submit}>
                <div className="field-label">
                  01 <span>Choose a task</span>
                </div>
                <div className="task-types">
                  {types.map((t) => (
                    <button
                      key={t.id}
                      className={`task-type ${selected.id === t.id ? "active" : ""}`}
                      type="button"
                      onClick={() => choose(t.id)}
                      aria-pressed={selected.id === t.id}
                    >
                      <t.icon size={18} />
                      <strong>{t.name}</strong>
                      <small>{t.caption}</small>
                      {selected.id === t.id && (
                        <span className="type-check">
                          <Check size={11} />
                        </span>
                      )}
                    </button>
                  ))}
                </div>
                <label className="field-label" htmlFor="payload">
                  02{" "}
                  <span>
                    {selected.id === "report.generate"
                      ? "Enter your numbers"
                      : selected.id === "benchmark.compute"
                        ? "Enter a seed"
                        : "Enter your text"}
                  </span>
                  <span className="mono">{selected.id}</span>
                </label>
                <div className="input-frame">
                  <div className="editor-head">
                    <span className="editor-dot" />
                    <span>payload</span>
                    <small>
                      {selected.id === "report.generate"
                        ? "number[]"
                        : "string"}
                    </small>
                  </div>
                  <textarea
                    id="payload"
                    value={input}
                    maxLength={6000}
                    onChange={(e) => setInput(e.target.value)}
                    spellCheck={false}
                    required
                  />
                  <div className="editor-foot">
                    <span>
                      {selected.id === "benchmark.compute"
                        ? "10,000 SHA-256 rounds"
                        : selected.id === "report.generate"
                          ? "Comma or space separated numbers"
                          : "Plain text · UTF-8 supported"}
                    </span>
                    <span>{input.length} chars</span>
                  </div>
                </div>
                <div className="options-row">
                  <label>
                    Jobs per run
                    <select
                      value={count}
                      onChange={(e) => setCount(Number(e.target.value))}
                    >
                      <option value="1">1 job</option>
                      <option value="5">5 jobs</option>
                      <option value="10">10 jobs</option>
                    </select>
                  </label>
                  <label>
                    Priority
                    <select
                      value={priority}
                      onChange={(e) => setPriority(Number(e.target.value))}
                    >
                      <option value="2">Low · 2</option>
                      <option value="5">Normal · 5</option>
                      <option value="9">High · 9</option>
                    </select>
                  </label>
                </div>
                <div className="submit-row">
                  <span>
                    <ShieldCheck size={14} />
                    {session
                      ? `${30 - session.submitted} of 30 jobs remaining`
                      : "Starting your private session…"}
                  </span>
                  <button
                    className="primary"
                    disabled={
                      !session || sending || count > 30 - session.submitted
                    }
                    type="submit"
                  >
                    {sending ? (
                      <LoaderCircle size={16} className="spin" />
                    ) : (
                      <Play size={15} />
                    )}{" "}
                    {sending
                      ? "Submitting…"
                      : "Submit job" + (count > 1 ? "s" : "")}
                    <ArrowRight size={16} />
                  </button>
                </div>
              </form>
            </section>
            <aside className="flow-panel">
              <div className="flow-top">
                <span className="overline">UNDER THE HOOD</span>
                <span className="outline-pill">
                  <span className="green-dot" />
                  At least once
                </span>
              </div>
              <h2>Durable by design.</h2>
              <p>
                A task is saved before it is acknowledged. Execution happens
                outside the request.
              </p>
              <div className="flow-step">
                <span className="flow-node">
                  <Code2 size={18} />
                </span>
                <div>
                  <strong>
                    01 <span>Submit</span>
                  </strong>
                  <small>Validated input, persisted in PostgreSQL</small>
                </div>
                <Check size={14} />
              </div>
              <div className="flow-line" />
              <div className="flow-step">
                <span className="flow-node">
                  <Layers3 size={18} />
                </span>
                <div>
                  <strong>
                    02 <span>Claim & execute</span>
                  </strong>
                  <small>Atomic claim, bounded worker concurrency</small>
                </div>
                <span className="green-dot" />
              </div>
              <div className="flow-line" />
              <div className="flow-step">
                <span className="flow-node">
                  <CheckCircle2 size={18} />
                </span>
                <div>
                  <strong>
                    03 <span>Commit result</span>
                  </strong>
                  <small>Only the current lease can acknowledge</small>
                </div>
              </div>
              <div className="flow-footer">
                <span className="bracket">{"{ }"}</span>
                <div>
                  <strong>No black boxes.</strong>
                  <small>Every attempt leaves a durable trail.</small>
                </div>
              </div>
            </aside>
          </div>
          <section className="panel activity-panel" id="activity">
            <div className="activity-head">
              <div className="tabs" role="tablist" aria-label="Console view">
                <button
                  role="tab"
                  aria-selected={tab === "jobs"}
                  className={tab === "jobs" ? "active" : ""}
                  onClick={() => setTab("jobs")}
                >
                  <Layers3 size={16} />
                  Job activity <span>{jobs.length}</span>
                </button>
                <button
                  role="tab"
                  aria-selected={tab === "workers"}
                  className={tab === "workers" ? "active" : ""}
                  onClick={() => setTab("workers")}
                >
                  <Server size={16} />
                  Workers
                </button>
              </div>
              <div className="activity-tools">
                {tab === "jobs" && (
                  <select
                    aria-label="Filter jobs"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                  >
                    <option value="all">All statuses</option>
                    <option value="completed">Completed</option>
                    <option value="active">Active</option>
                  </select>
                )}
                <button
                  className="icon-button"
                  onClick={() => void refresh()}
                  aria-label="Refresh activity"
                >
                  <RefreshCw size={15} />
                </button>
              </div>
            </div>
            {tab === "jobs" ? (
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>JOB / TYPE</th>
                      <th>STATUS</th>
                      <th>QUEUE</th>
                      <th>ATTEMPTS</th>
                      <th>END-TO-END</th>
                      <th>CREATED</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((job) => (
                      <tr key={job.id} onClick={() => void inspect(job)}>
                        <td>
                          <button
                            className="job-link"
                            onClick={(e) => {
                              e.stopPropagation();
                              void inspect(job);
                            }}
                          >
                            <span className="mono job-id">
                              {job.id.slice(0, 8)}
                            </span>
                            <small>{job.type}</small>
                          </button>
                        </td>
                        <td>
                          <span
                            className={`status ${job.status.toLowerCase()}`}
                          >
                            <span />
                            {statusLabel(job.status)}
                          </span>
                        </td>
                        <td>
                          <span className="queue-label">{job.queue}</span>
                        </td>
                        <td className="mono muted">
                          {job.attempt_count} / {job.max_attempts}
                        </td>
                        <td className="mono muted">{duration(job)}</td>
                        <td className="mono muted">{time(job.created_at)}</td>
                        <td>
                          <ChevronRight size={15} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {shown.length === 0 && (
                  <div className="empty">
                    <div className="empty-icon">
                      <Boxes size={27} strokeWidth={1.3} />
                    </div>
                    <h3>
                      {jobs.length
                        ? "No jobs match this filter"
                        : "Your next task starts here"}
                    </h3>
                    <p>
                      {jobs.length
                        ? "Choose another status to see your jobs."
                        : "Submit your first job above and watch it move through the queue."}
                    </p>
                    <span>
                      <ArrowRight size={13} /> Real execution · Results saved to
                      PostgreSQL
                    </span>
                  </div>
                )}
              </div>
            ) : (
              <div className="worker-grid">
                {workers.map((w) => (
                  <article className="worker" key={w.name}>
                    <div className="worker-icon">
                      <Server size={22} />
                      <span className={w.online ? "green-dot" : "amber-dot"} />
                    </div>
                    <h3>{w.name}</h3>
                    <span
                      className={`status ${w.online ? "succeeded" : "pending"}`}
                    >
                      <span />
                      {w.online ? "Online" : "Offline"}
                    </span>
                    <dl>
                      <div>
                        <dt>Concurrency</dt>
                        <dd>{w.concurrency}</dd>
                      </div>
                      <div>
                        <dt>Active leases</dt>
                        <dd>{w.active}</dd>
                      </div>
                      <div>
                        <dt>Last heartbeat</dt>
                        <dd>{time(w.heartbeat_at)}</dd>
                      </div>
                    </dl>
                  </article>
                ))}
              </div>
            )}
            <div className="table-footer">
              <span>
                <span className="green-dot" />
                Your session only · Updates every 2.5s
              </span>
              <span>
                {session
                  ? `Saved until ${new Date(session.expires_at).toLocaleString()}`
                  : "No sign-up required"}
              </span>
            </div>
          </section>
          <section className="guarantees" id="guarantees">
            <div>
              <ShieldCheck size={19} />
              <h3>Built for the unexpected.</h3>
            </div>
            <p>
              Lease renewal, fencing tokens and retries protect the job state
              when a worker disappears. Delivery is at least once; exactly-once
              external side effects are not promised.
            </p>
            <div className="guarantee-tags">
              <span>Atomic claims</span>
              <span>Lease fencing</span>
              <span>Exponential backoff</span>
              <span>Crash recovery</span>
            </div>
          </section>
          <footer>
            <span>
              <Mark />
              TaskMesh <span className="footer-sep">/</span> Built by Adam
              Sapiecha
            </span>
            <a href={portfolio + "projects.html#taskmesh"}>
              About this project <ArrowRight size={13} />
            </a>
            <p>
              Your jobs are private to this browser session and expire after 24
              hours. This public workspace is separate from the owner's data. Do
              not submit sensitive information.
            </p>
          </footer>
        </main>
      </div>
      {detail && (
        <div className="drawer-backdrop" onClick={close}>
          <section
            className="drawer"
            role="dialog"
            aria-modal="true"
            aria-labelledby="job-detail-title"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="drawer-top">
              <span className="overline">EXECUTION DETAILS</span>
              <button
                className="icon-button"
                onClick={close}
                aria-label="Close details"
              >
                <X size={20} />
              </button>
            </div>
            <h2 id="job-detail-title">{detail.type}</h2>
            <span className={`status ${detail.status.toLowerCase()}`}>
              <span />
              {statusLabel(detail.status)}
            </span>
            <p className="mono full-id">{detail.id}</p>
            <div className="detail-meta">
              <div>
                <small>Queue</small>
                <strong>{detail.queue}</strong>
              </div>
              <div>
                <small>Priority</small>
                <strong>{detail.priority}</strong>
              </div>
              <div>
                <small>Lease generation</small>
                <strong>{detail.lease_generation}</strong>
              </div>
              <div>
                <small>End-to-end</small>
                <strong>{duration(detail)}</strong>
              </div>
            </div>
            <h3>Input</h3>
            <pre>{JSON.stringify(detail.payload, null, 2)}</pre>
            <h3>Result</h3>
            <pre className="result-block">
              {detail.result
                ? JSON.stringify(detail.result, null, 2)
                : detail.last_error || "Waiting for the worker to finish…"}
            </pre>
            <h3>
              Execution trail{" "}
              <span className="muted">
                {attempts.length} attempt{attempts.length === 1 ? "" : "s"}
              </span>
            </h3>
            <ol className="timeline">
              {events.map((event) => (
                <li key={event.id}>
                  <span
                    className={
                      event.status === "SUCCEEDED"
                        ? "green-dot"
                        : "timeline-dot"
                    }
                  />
                  <div>
                    <strong>{statusLabel(event.status)}</strong>
                    <small>Generation {event.lease_generation}</small>
                  </div>
                  <time>{time(event.created_at)}</time>
                </li>
              ))}
            </ol>
            {attempts.some((a) => a.error) && (
              <div className="error">
                {attempts
                  .filter((a) => a.error)
                  .map((a) => (
                    <p key={a.generation}>{a.error}</p>
                  ))}
              </div>
            )}
            {!terminal.has(detail.status) && (
              <button
                className="secondary danger"
                onClick={async () => {
                  try {
                    await api(`jobs/${detail.id}/cancel`, {
                      method: "POST",
                      body: "{}",
                    });
                    await refresh();
                  } catch (e) {
                    setError((e as Error).message);
                  }
                }}
              >
                <XCircle size={16} />
                Cancel job
              </button>
            )}
          </section>
        </div>
      )}
    </div>
  );
}
createRoot(document.getElementById("root")!).render(<App />);
