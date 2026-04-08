import type { DebugSnapshot, ModelsResponse, DownloadProgressData } from './types';

export async function fetchDebugMetrics(): Promise<DebugSnapshot> {
  const res = await fetch('/api/debug');
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export async function fetchModels(): Promise<ModelsResponse> {
  const res = await fetch('/api/models');
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export interface InferenceCallbacks {
  onToken: (token: string) => void;
  onDone: () => void;
  onError: (error: string) => void;
  onProcessing: () => void;
}

export function startInference(
  prompt: string,
  local: boolean,
  callbacks: InferenceCallbacks
): AbortController {
  const controller = new AbortController();

  fetch('/api/inference', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, local }),
    signal: controller.signal,
  })
    .then(async (res) => {
      if (!res.ok) {
        callbacks.onError(`HTTP ${res.status}`);
        return;
      }
      const reader = res.body?.getReader();
      if (!reader) {
        callbacks.onError('No response body');
        return;
      }

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        let currentEvent = '';
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            const data = line.slice(6);
            switch (currentEvent) {
              case 'status':
                callbacks.onProcessing();
                break;
              case 'token':
                callbacks.onToken(data);
                break;
              case 'done':
                callbacks.onDone();
                break;
              case 'error':
                callbacks.onError(data);
                break;
            }
            currentEvent = '';
          }
        }
      }
    })
    .catch((err) => {
      if (err.name !== 'AbortError') {
        callbacks.onError(err.message);
      }
    });

  return controller;
}

export async function cancelInference(): Promise<void> {
  await fetch('/api/inference', { method: 'DELETE' });
}

export async function startDownload(fileName: string): Promise<void> {
  const res = await fetch('/api/models/download', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fileName }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

export async function cancelDownload(): Promise<void> {
  await fetch('/api/models/download', { method: 'DELETE' });
}

export function subscribeDownloadProgress(
  onProgress: (data: DownloadProgressData) => void,
  onError?: (err: string) => void
): EventSource {
  const es = new EventSource('/api/models/download/progress');

  es.addEventListener('progress', (e) => {
    try {
      const data: DownloadProgressData = JSON.parse((e as MessageEvent).data);
      onProgress(data);
    } catch {
      // ignore parse errors
    }
  });

  es.onerror = () => {
    onError?.('Connection lost');
  };

  return es;
}
