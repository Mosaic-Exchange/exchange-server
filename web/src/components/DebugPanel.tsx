import { useEffect, useState } from 'react';
import type { DebugSnapshot } from '../types';
import { fetchDebugMetrics } from '../api';

function formatUptime(ms: number): string {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return `${h}h ${m % 60}m ${s % 60}s`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

function formatElapsed(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export default function DebugPanel() {
  const [data, setData] = useState<DebugSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const poll = () => {
      fetchDebugMetrics()
        .then((d) => { if (active) { setData(d); setError(null); } })
        .catch((e) => { if (active) setError(e.message); });
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => { active = false; clearInterval(id); };
  }, []);

  if (error) return <div className="panel-error">Failed to load metrics: {error}</div>;
  if (!data) return <div className="panel-loading">Loading metrics...</div>;

  return (
    <div className="debug-panel">
      {/* Node Info */}
      <div className="debug-section">
        <h3>Node</h3>
        <div className="debug-kv">
          <span className="label">ID</span>
          <span className="value mono">{data.node}</span>
        </div>
        <div className="debug-kv">
          <span className="label">Uptime</span>
          <span className="value">{formatUptime(data.uptimeMs)}</span>
        </div>
      </div>

      {/* Request Counts */}
      <div className="debug-section">
        <h3>Requests</h3>
        <div className="debug-stats-grid">
          <div className="stat-card">
            <div className="stat-value">{data.requests.pendingOutbound}</div>
            <div className="stat-label">Pending Outbound</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">{data.requests.activeStreamsServer}</div>
            <div className="stat-label">Active Streams (Server)</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">{data.requests.pendingHandshakes}</div>
            <div className="stat-label">Pending Handshakes</div>
          </div>
        </div>
      </div>

      {/* Active Operations */}
      <div className="debug-section">
        <h3>Active Operations</h3>
        <div className="debug-kv">
          <span className="label">Inference</span>
          <span className={`value badge ${data.activeOperations.inferenceRunning ? 'badge-active' : 'badge-idle'}`}>
            {data.activeOperations.inferenceRunning ? 'Running' : 'Idle'}
          </span>
        </div>
        <div className="debug-kv">
          <span className="label">Download</span>
          <span className={`value badge ${data.activeOperations.downloadRunning ? 'badge-active' : 'badge-idle'}`}>
            {data.activeOperations.downloadRunning ? 'Running' : 'Idle'}
          </span>
        </div>
        {data.activeOperations.download && (
          <div className="debug-kv">
            <span className="label">Download File</span>
            <span className="value mono">{data.activeOperations.download.fileName}</span>
          </div>
        )}
      </div>

      {/* Pending Request Details */}
      {data.pendingDetails.length > 0 && (
        <div className="debug-section">
          <h3>Pending Request Details</h3>
          <table className="debug-table">
            <thead>
              <tr>
                <th>Request ID</th>
                <th>Type</th>
                <th>Elapsed</th>
              </tr>
            </thead>
            <tbody>
              {data.pendingDetails.map((d) => (
                <tr key={d.requestId}>
                  <td className="mono">{d.requestId}</td>
                  <td>{d.streaming ? 'Streaming' : 'Request/Response'}</td>
                  <td>{formatElapsed(d.elapsedMs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Executor Stats */}
      {Object.keys(data.executors).length > 0 && (
        <div className="debug-section">
          <h3>Service Executors</h3>
          {Object.entries(data.executors).map(([name, pair]) => (
            <div key={name} className="executor-group">
              <h4>{name}</h4>
              <div className="executor-pair">
                <div className="executor-card">
                  <div className="executor-title">Remote Pool</div>
                  <div className="executor-stats">
                    <span>Active: <strong>{pair.remote.activeThreads}</strong>/{pair.remote.maxPoolSize}</span>
                    <span>Pool: {pair.remote.poolSize}</span>
                    <span>Queue: {pair.remote.queueSize}</span>
                    <span>Completed: {pair.remote.completedTasks}</span>
                  </div>
                </div>
                <div className="executor-card">
                  <div className="executor-title">Local Pool</div>
                  <div className="executor-stats">
                    <span>Active: <strong>{pair.local.activeThreads}</strong>/{pair.local.maxPoolSize}</span>
                    <span>Pool: {pair.local.poolSize}</span>
                    <span>Queue: {pair.local.queueSize}</span>
                    <span>Completed: {pair.local.completedTasks}</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Cluster Topology */}
      <div className="debug-section">
        <h3>Cluster Topology</h3>
        <table className="debug-table">
          <thead>
            <tr>
              <th>Node</th>
              <th>Type</th>
              <th>Status</th>
              <th>Heartbeat</th>
              <th>Services</th>
            </tr>
          </thead>
          <tbody>
            {data.cluster.map((node) => (
              <tr key={node.id} className={node.self ? 'row-self' : ''}>
                <td className="mono">
                  {node.id}
                  {node.self && <span className="self-badge">self</span>}
                </td>
                <td>{node.type}</td>
                <td>
                  <span className={`badge ${node.status === 'ALIVE' ? 'badge-active' : 'badge-down'}`}>
                    {node.status}
                  </span>
                </td>
                <td className="mono">{node.heartbeat}</td>
                <td className="mono">{node.services || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
