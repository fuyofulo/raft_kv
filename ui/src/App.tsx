import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

type Role = "Leader" | "Follower" | "Candidate" | "Unknown";

type RaftEvent = {
  timestamp_ms: number;
  node_id: number;
  kind: string;
  message: string;
  term: number;
  commit_index: number;
  last_applied: number;
  log_len: number;
};

type NodeView = {
  id: number;
  enabled: boolean;
  role: Role;
  term: number;
  commitIndex: number;
  lastApplied: number;
  logLen: number;
  recent: RaftEvent[];
};

type ClusterState = {
  enabled_nodes: Record<string, boolean>;
  blocked_links: [number, number][];
  events_count: number;
};

type ClientResult = {
  ok: boolean;
  message: string;
  target_node: number;
  final_node: number;
  response: Record<string, unknown>;
};

type SelectOption = {
  value: string;
  label: string;
};

type Toast = {
  id: number;
  kind: "ok" | "error" | "info";
  message: string;
};

const NODE_IDS = [1, 2, 3, 4, 5];
const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:7000";

function initialNodes(): Record<number, NodeView> {
  return Object.fromEntries(
    NODE_IDS.map((id) => [
      id,
      {
        id,
        enabled: true,
        role: "Unknown",
        term: 0,
        commitIndex: 0,
        lastApplied: 0,
        logLen: 0,
        recent: []
      }
    ])
  ) as Record<number, NodeView>;
}

function eventRole(event: RaftEvent): Role | null {
  if (event.kind !== "role_change") return null;
  const msg = event.message.toLowerCase();
  if (msg.includes("leader")) return "Leader";
  if (msg.includes("follower")) return "Follower";
  if (msg.includes("candidate")) return "Candidate";
  return null;
}

function applyEvent(nodes: Record<number, NodeView>, event: RaftEvent): Record<number, NodeView> {
  const current = nodes[event.node_id];
  if (!current) return nodes;

  const nextRole = eventRole(event) ?? current.role;
  const latest = current.recent[0];
  const isDuplicate =
    latest &&
    latest.kind === event.kind &&
    latest.message === event.message &&
    latest.term === event.term &&
    latest.commit_index === event.commit_index &&
    latest.last_applied === event.last_applied &&
    latest.log_len === event.log_len;
  const recent = isDuplicate ? current.recent : [event, ...current.recent].slice(0, 10);
  return {
    ...nodes,
    [event.node_id]: {
      ...current,
      role: nextRole,
      term: event.term,
      commitIndex: event.commit_index,
      lastApplied: event.last_applied,
      logLen: event.log_len,
      recent
    }
  };
}

async function postJson<T>(path: string, body?: unknown): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: body ? JSON.stringify(body) : "{}"
  });
  if (!resp.ok) {
    throw new Error(`request failed: ${resp.status}`);
  }
  return (await resp.json()) as T;
}

function RetroSelect({
  id,
  value,
  options,
  onChange
}: {
  id: string;
  value: string;
  options: SelectOption[];
  onChange: (next: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const selected = options.find((x) => x.value === value) ?? options[0];

  return (
    <div
      className={`retro-select ${open ? "open" : ""}`}
      tabIndex={0}
      onBlur={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
          setOpen(false);
        }
      }}
    >
      <button
        id={id}
        type="button"
        className="retro-select-trigger"
        onClick={() => setOpen((prev) => !prev)}
      >
        <span>{selected?.label ?? value}</span>
        <span className="retro-select-caret">▾</span>
      </button>
      {open && (
        <div className="retro-select-menu" role="listbox">
          {options.map((opt) => (
            <button
              key={opt.value}
              type="button"
              className={`retro-select-item ${opt.value === value ? "active" : ""}`}
              onClick={() => {
                onChange(opt.value);
                setOpen(false);
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function App() {
  const [nodes, setNodes] = useState<Record<number, NodeView>>(initialNodes);
  const [eventsFeed, setEventsFeed] = useState<RaftEvent[]>([]);
  const [cluster, setCluster] = useState<ClusterState>({
    enabled_nodes: {},
    blocked_links: [],
    events_count: 0
  });
  const [groupA, setGroupA] = useState("1,2");
  const [groupB, setGroupB] = useState("3,4,5");
  const [toasts, setToasts] = useState<Toast[]>([]);

  const [targetNode, setTargetNode] = useState<number>(1);
  const [op, setOp] = useState<"put" | "update" | "delete" | "get">("put");
  const [key, setKey] = useState("x");
  const [value, setValue] = useState("10");
  const [clientId, setClientId] = useState("1");
  const [requestId, setRequestId] = useState("");
  const [clientResults, setClientResults] = useState<ClientResult[]>([]);
  const clientResultsRef = useRef<HTMLDivElement | null>(null);

  const pushToast = (message: string, kind: Toast["kind"] = "info") => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setToasts((prev) => [{ id, kind, message }, ...prev].slice(0, 4));
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 3200);
  };

  useEffect(() => {
    let closed = false;

    const loadHistory = async () => {
      try {
        const resp = await fetch(`${API_BASE}/events/history`);
        const history = (await resp.json()) as RaftEvent[];
        if (closed) return;
        setEventsFeed(history.slice(-200).reverse());
        setNodes((prev) => history.reduce((acc, event) => applyEvent(acc, event), prev));
      } catch {
        pushToast("failed to load event history", "error");
      }
    };

    const loadState = async () => {
      try {
        const resp = await fetch(`${API_BASE}/state`);
        const next = (await resp.json()) as ClusterState;
        if (closed) return;
        setCluster(next);
        setNodes((prev) => {
          const updated = { ...prev };
          for (const id of NODE_IDS) {
            updated[id] = {
              ...updated[id],
              enabled: next.enabled_nodes[String(id)] ?? true
            };
          }
          return updated;
        });
      } catch {
        pushToast("failed to load cluster state", "error");
      }
    };

    void loadHistory();
    void loadState();
    const poll = window.setInterval(() => {
      void loadState();
    }, 1000);

    const stream = new EventSource(`${API_BASE}/events/stream`);
    stream.addEventListener("raft_event", (raw) => {
      const payload = JSON.parse((raw as MessageEvent).data) as RaftEvent;
      if (closed) return;
      setNodes((prev) => applyEvent(prev, payload));
      setEventsFeed((prev) => [payload, ...prev].slice(0, 200));
    });
    stream.onerror = () => {
      pushToast("event stream disconnected", "error");
    };

    return () => {
      closed = true;
      stream.close();
      window.clearInterval(poll);
    };
  }, []);

  const nodeList = useMemo(() => NODE_IDS.map((id) => nodes[id]), [nodes]);

  useEffect(() => {
    if (clientResultsRef.current) {
      clientResultsRef.current.scrollTop = 0;
    }
  }, [clientResults]);

  const submitClientCommand = async (e: FormEvent) => {
    e.preventDefault();
    try {
      const payload: Record<string, unknown> = {
        target_node: targetNode,
        op,
        key
      };
      if (op === "put" || op === "update") payload.value = value;
      if (clientId.trim() !== "") payload.client_id = Number(clientId);
      if (requestId.trim() !== "") payload.request_id = Number(requestId);

      const resp = await postJson<ClientResult>("/client/command", payload);
      setClientResults((prev) => [resp, ...prev].slice(0, 20));
      pushToast(`client command finished (ok=${resp.ok})`, resp.ok ? "ok" : "error");
    } catch (err) {
      pushToast(`client command failed: ${(err as Error).message}`, "error");
    }
  };

  const stopNode = async (id: number) => {
    try {
      await postJson(`/nodes/${id}/stop`);
      setNodes((prev) => ({
        ...prev,
        [id]: {
          ...prev[id],
          enabled: false
        }
      }));
      pushToast(`node ${id} stopped`, "ok");
    } catch (err) {
      pushToast(`stop failed: ${(err as Error).message}`, "error");
    }
  };

  const startNode = async (id: number) => {
    try {
      await postJson(`/nodes/${id}/start`);
      setNodes((prev) => ({
        ...prev,
        [id]: {
          ...prev[id],
          enabled: true
        }
      }));
      pushToast(`node ${id} started`, "ok");
    } catch (err) {
      pushToast(`start failed: ${(err as Error).message}`, "error");
    }
  };

  const applyPartition = async () => {
    try {
      const group_a = groupA
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
      const group_b = groupB
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
      await postJson("/partition", { group_a, group_b });
      pushToast(`partition applied A=[${group_a}] B=[${group_b}]`, "ok");
    } catch (err) {
      pushToast(`partition failed: ${(err as Error).message}`, "error");
    }
  };

  const healNetwork = async () => {
    try {
      await postJson("/heal");
      pushToast("network healed", "ok");
    } catch (err) {
      pushToast(`heal failed: ${(err as Error).message}`, "error");
    }
  };

  return (
    <div className="screen">
      <header className="topbar">
        <div className="title">RAFT_KV // @fuyofulo</div>
        <div className="meta">api={API_BASE}</div>
      </header>

      <div className="toast-stack">
        {toasts.map((toast) => (
          <div className={`toast ${toast.kind}`} key={toast.id}>
            {toast.message}
          </div>
        ))}
      </div>

      <main className="layout">
        <section className="left">
          <div className="panel-title">NODES (5)</div>
          <div className="nodes-grid">
            {nodeList.map((node) => (
              <article className="node-card" key={node.id}>
                <div className="node-head">
                  <span>node-{node.id}</span>
                  <span className={`role ${node.enabled ? node.role.toLowerCase() : "offline"}`}>
                    {node.enabled ? node.role : "Offline"}
                  </span>
                </div>
                <div className="kv">
                  <div>status: {node.enabled ? "online" : "offline"}</div>
                  <div>term: {node.term}</div>
                  <div>commit: {node.commitIndex}</div>
                  <div>applied: {node.lastApplied}</div>
                  <div>log_len: {node.logLen}</div>
                </div>
                <div className="node-actions">
                  <button disabled={node.enabled} onClick={() => startNode(node.id)}>
                    start
                  </button>
                  <button disabled={!node.enabled} onClick={() => stopNode(node.id)}>
                    stop
                  </button>
                </div>
                <div className="events">
                  {node.recent.length === 0 ? (
                    <div className="event-line muted">
                      {node.enabled ? "(no events yet)" : "(offline)"}
                    </div>
                  ) : (
                    node.recent.map((ev, idx) => (
                      <div className="event-line" key={`${ev.timestamp_ms}-${idx}`}>
                        [{new Date(ev.timestamp_ms).toLocaleTimeString()}] {ev.kind}: {ev.message}
                      </div>
                    ))
                  )}
                </div>
              </article>
            ))}
            <article className="node-card feed-node-card">
              <div className="panel-title">GLOBAL FEED</div>
              <div className="events tight">
                {eventsFeed.map((ev, idx) => (
                  <div className="event-line" key={`${ev.timestamp_ms}-${idx}`}>
                    n{ev.node_id} t{ev.term} {ev.kind}: {ev.message}
                  </div>
                ))}
              </div>
            </article>
          </div>
        </section>

        <aside className="right">
          <section className="panel network-panel">
            <div className="panel-title">NETWORK CONTROL</div>
            <div className="form-grid">
              <div className="field-row">
                <label htmlFor="group-a">group A</label>
                <input id="group-a" value={groupA} onChange={(e) => setGroupA(e.target.value)} />
              </div>
              <div className="field-row">
                <label htmlFor="group-b">group B</label>
                <input id="group-b" value={groupB} onChange={(e) => setGroupB(e.target.value)} />
              </div>
            </div>
            <div className="row">
              <button onClick={applyPartition}>apply partition</button>
              <button onClick={healNetwork}>heal</button>
            </div>
            <div className="muted">blocked links: {cluster.blocked_links.length}</div>
          </section>

          <section className="panel client-panel">
            <div className="panel-title">CLIENT PANEL</div>
            <form onSubmit={submitClientCommand}>
              <div className="form-grid">
                <div className="field-row">
                  <label htmlFor="target-node">target node</label>
                  <RetroSelect
                    id="target-node"
                    value={String(targetNode)}
                    onChange={(next) => setTargetNode(Number(next))}
                    options={NODE_IDS.map((id) => ({ value: String(id), label: `node-${id}` }))}
                  />
                </div>
                <div className="field-row">
                  <label htmlFor="op">op</label>
                  <RetroSelect
                    id="op"
                    value={op}
                    onChange={(next) => setOp(next as typeof op)}
                    options={[
                      { value: "put", label: "put" },
                      { value: "update", label: "update" },
                      { value: "delete", label: "delete" },
                      { value: "get", label: "get" }
                    ]}
                  />
                </div>
                <div className="field-row">
                  <label htmlFor="key">key</label>
                  <input id="key" value={key} onChange={(e) => setKey(e.target.value)} required />
                </div>
                {(op === "put" || op === "update") && (
                  <div className="field-row">
                    <label htmlFor="value">value</label>
                    <input id="value" value={value} onChange={(e) => setValue(e.target.value)} />
                  </div>
                )}
                <div className="field-row">
                  <label htmlFor="client-id">client id</label>
                  <input
                    id="client-id"
                    value={clientId}
                    onChange={(e) => setClientId(e.target.value)}
                  />
                </div>
                <div className="field-row">
                  <label htmlFor="request-id">request id</label>
                  <input
                    id="request-id"
                    value={requestId}
                    onChange={(e) => setRequestId(e.target.value)}
                    placeholder="optional"
                  />
                </div>
              </div>
              {(op === "put" || op === "update") && (
                <div className="hint">put/update requires value</div>
              )}
              <button type="submit" className="submit-btn">
                run command
              </button>
            </form>
            <div className="events tight" ref={clientResultsRef}>
              {clientResults.map((res, idx) => (
                <div className="event-line client-line" key={idx}>
                  ok={String(res.ok)} target={res.target_node} final={res.final_node} {res.message}
                  <span className="result-json">{JSON.stringify(res.response)}</span>
                </div>
              ))}
            </div>
          </section>

        </aside>
      </main>
    </div>
  );
}
