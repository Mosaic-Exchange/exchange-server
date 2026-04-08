export interface ExecutorSnapshot {
  activeThreads: number;
  poolSize: number;
  maxPoolSize: number;
  queueSize: number;
  completedTasks: number;
}

export interface ExecutorPair {
  remote: ExecutorSnapshot;
  local: ExecutorSnapshot;
}

export interface PendingRequestDetail {
  requestId: number;
  streaming: boolean;
  elapsedMs: number;
}

export interface ClusterNode {
  id: string;
  type: string;
  status: string;
  services: string;
  heartbeat: number;
  self: boolean;
  appStates: Record<string, string>;
}

export interface DownloadProgressData {
  fileName: string;
  bytesReceived: number;
  totalBytes: number;
  status: string;
}

export interface ActiveOperations {
  inferenceRunning: boolean;
  downloadRunning: boolean;
  download?: DownloadProgressData;
}

export interface DebugSnapshot {
  node: string;
  uptimeMs: number;
  requests: {
    pendingOutbound: number;
    activeStreamsServer: number;
    pendingHandshakes: number;
  };
  pendingDetails: PendingRequestDetail[];
  executors: Record<string, ExecutorPair>;
  activeOperations: ActiveOperations;
  cluster: ClusterNode[];
}

export interface PeerFile {
  name: string;
  size: number;
}

export interface PeerFiles {
  nodeId: string;
  files: PeerFile[];
}

export interface ModelsResponse {
  peers: PeerFiles[];
}
