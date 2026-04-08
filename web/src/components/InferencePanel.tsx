import { useState, useRef, useEffect, useCallback } from 'react';
import { startInference, cancelInference } from '../api';

type Status = 'idle' | 'processing' | 'streaming' | 'done' | 'error' | 'cancelled';

export default function InferencePanel() {
  const [prompt, setPrompt] = useState('');
  const [output, setOutput] = useState('');
  const [status, setStatus] = useState<Status>('idle');
  const [error, setError] = useState<string | null>(null);
  const [isLocal, setIsLocal] = useState(true);
  const outputRef = useRef<HTMLPreElement>(null);
  const controllerRef = useRef<AbortController | null>(null);

  // Auto-scroll output
  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [output]);

  const handleSend = useCallback(() => {
    if (!prompt.trim()) return;

    setOutput('');
    setError(null);
    setStatus('processing');

    const controller = startInference(prompt, isLocal, {
      onProcessing: () => setStatus('streaming'),
      onToken: (token) => {
        setStatus('streaming');
        setOutput((prev) => prev + token);
      },
      onDone: () => setStatus('done'),
      onError: (err) => {
        setError(err);
        setStatus('error');
      },
    });

    controllerRef.current = controller;
  }, [prompt, isLocal]);

  const handleCancel = useCallback(async () => {
    controllerRef.current?.abort();
    controllerRef.current = null;
    await cancelInference().catch(() => {});
    setStatus('cancelled');
  }, []);

  const handleNew = useCallback(() => {
    controllerRef.current?.abort();
    controllerRef.current = null;
    setPrompt('');
    setOutput('');
    setError(null);
    setStatus('idle');
  }, []);

  const isRunning = status === 'processing' || status === 'streaming';

  return (
    <div className="inference-panel">
      <div className="inference-header">
        <h3>Inference</h3>
        <div className="inference-mode-toggle">
          <button
            className={`toggle-btn ${isLocal ? 'active' : ''}`}
            onClick={() => setIsLocal(true)}
            disabled={isRunning}
          >
            Local
          </button>
          <button
            className={`toggle-btn ${!isLocal ? 'active' : ''}`}
            onClick={() => setIsLocal(false)}
            disabled={isRunning}
          >
            Remote
          </button>
        </div>
      </div>

      <div className="inference-input-row">
        <textarea
          className="inference-prompt"
          placeholder="Enter your prompt..."
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey && !isRunning) {
              e.preventDefault();
              handleSend();
            }
          }}
          disabled={isRunning}
          rows={3}
        />
      </div>

      <div className="inference-actions">
        {!isRunning && (
          <button className="btn btn-primary" onClick={handleSend} disabled={!prompt.trim()}>
            Send
          </button>
        )}
        {isRunning && (
          <button className="btn btn-danger" onClick={handleCancel}>
            Cancel
          </button>
        )}
        <button className="btn btn-secondary" onClick={handleNew}>
          New
        </button>
        <span className={`inference-status status-${status}`}>
          {status === 'idle' && 'Ready'}
          {status === 'processing' && 'Connecting...'}
          {status === 'streaming' && 'Generating...'}
          {status === 'done' && 'Complete'}
          {status === 'error' && 'Error'}
          {status === 'cancelled' && 'Cancelled'}
        </span>
      </div>

      {(output || error) && (
        <div className="inference-output-container">
          <h4>Response</h4>
          <pre ref={outputRef} className="inference-output">
            {output}
            {isRunning && <span className="cursor">|</span>}
          </pre>
          {error && <div className="inference-error">{error}</div>}
        </div>
      )}
    </div>
  );
}
