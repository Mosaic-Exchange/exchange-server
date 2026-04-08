import { useEffect, useState, useCallback, useRef } from 'react';
import type { PeerFiles, DownloadProgressData } from '../types';
import { fetchModels, startDownload, cancelDownload, subscribeDownloadProgress } from '../api';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export default function ModelsPanel() {
  const [peers, setPeers] = useState<PeerFiles[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<DownloadProgressData | null>(null);
  const esRef = useRef<EventSource | null>(null);

  const loadModels = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchModels();
      setPeers(res.peers);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadModels();
  }, [loadModels]);

  const handleDownload = useCallback(async (fileName: string) => {
    try {
      setError(null);
      setProgress({ fileName, bytesReceived: 0, totalBytes: 0, status: 'starting' });

      // Subscribe to progress updates
      if (esRef.current) esRef.current.close();
      esRef.current = subscribeDownloadProgress(
        (data) => setProgress(data),
        (err) => setError(err)
      );

      await startDownload(fileName);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Download failed');
      setProgress(null);
    }
  }, []);

  const handleCancel = useCallback(async () => {
    try {
      await cancelDownload();
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    } catch {
      // ignore
    }
  }, []);

  // Cleanup EventSource on unmount
  useEffect(() => {
    return () => {
      if (esRef.current) esRef.current.close();
    };
  }, []);

  const isDownloading = progress != null &&
    (progress.status === 'downloading' || progress.status === 'starting' || progress.status === 'pending');

  const isTerminal = progress != null &&
    (progress.status === 'completed' || progress.status.startsWith('failed') || progress.status === 'cancelled');

  const progressPercent = progress && progress.totalBytes > 0
    ? Math.min(100, (progress.bytesReceived / progress.totalBytes) * 100)
    : 0;

  const totalFiles = peers.reduce((sum, p) => sum + p.files.length, 0);

  return (
    <div className="models-panel">
      <div className="models-header">
        <h3>Model Adapters</h3>
        <button className="btn btn-secondary" onClick={loadModels} disabled={loading}>
          {loading ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      {error && <div className="panel-error">{error}</div>}

      {/* Download Progress */}
      {progress && (
        <div className="download-card">
          <div className="download-info">
            <div className="download-filename">{progress.fileName}</div>
            <div className="download-status-row">
              <span className={`badge ${isDownloading ? 'badge-active' : isTerminal ? (progress.status === 'completed' ? 'badge-success' : 'badge-down') : 'badge-idle'}`}>
                {progress.status === 'completed' ? 'Completed' :
                 progress.status === 'cancelled' ? 'Cancelled' :
                 progress.status.startsWith('failed') ? 'Failed' :
                 'Downloading'}
              </span>
              <span className="download-bytes">
                {formatBytes(progress.bytesReceived)}
                {progress.totalBytes > 0 && ` / ${formatBytes(progress.totalBytes)}`}
              </span>
            </div>
          </div>

          <div className="progress-bar-container">
            <div
              className={`progress-bar ${progress.status === 'completed' ? 'progress-done' : ''}`}
              style={{ width: `${progress.totalBytes > 0 ? progressPercent : (isDownloading ? 100 : 0)}%` }}
            />
            {progress.totalBytes > 0 && (
              <span className="progress-percent">{progressPercent.toFixed(1)}%</span>
            )}
          </div>

          {isDownloading && (
            <button className="btn btn-danger btn-sm" onClick={handleCancel}>
              Cancel Download
            </button>
          )}

          {progress.status.startsWith('failed') && (
            <div className="download-error">
              {progress.status.replace('failed:', 'Error: ')}
            </div>
          )}
        </div>
      )}

      {/* Available Models */}
      {peers.length === 0 && !loading && (
        <div className="models-empty">
          No model adapters found on remote peers. Make sure peers have files in their shared directory.
        </div>
      )}

      {peers.length > 0 && (
        <div className="models-list">
          <div className="models-summary">{totalFiles} adapter{totalFiles !== 1 ? 's' : ''} available across {peers.length} peer{peers.length !== 1 ? 's' : ''}</div>
          {peers.map((peer) => (
            <div key={peer.nodeId} className="peer-group">
              <div className="peer-header">
                <span className="peer-id mono">{peer.nodeId}</span>
                <span className="peer-count">{peer.files.length} file{peer.files.length !== 1 ? 's' : ''}</span>
              </div>
              <table className="debug-table models-table">
                <thead>
                  <tr>
                    <th>Adapter Name</th>
                    <th>Size</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {peer.files.map((file) => (
                    <tr key={file.name}>
                      <td className="mono">{file.name}</td>
                      <td>{formatBytes(file.size)}</td>
                      <td>
                        <button
                          className="btn btn-primary btn-sm"
                          onClick={() => handleDownload(file.name)}
                          disabled={isDownloading}
                        >
                          Download
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
