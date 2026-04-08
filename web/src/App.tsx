import { useState } from 'react';
import DebugPanel from './components/DebugPanel';
import InferencePanel from './components/InferencePanel';
import ModelsPanel from './components/ModelsPanel';
import './App.css';

type Tab = 'inference' | 'models' | 'debug';

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('inference');

  return (
    <div className="app">
      <header className="app-header">
        <h1>Mosaic</h1>
        <nav className="app-nav">
          <button
            className={`nav-tab ${activeTab === 'inference' ? 'active' : ''}`}
            onClick={() => setActiveTab('inference')}
          >
            Inference
          </button>
          <button
            className={`nav-tab ${activeTab === 'models' ? 'active' : ''}`}
            onClick={() => setActiveTab('models')}
          >
            Model Adapters
          </button>
          <button
            className={`nav-tab ${activeTab === 'debug' ? 'active' : ''}`}
            onClick={() => setActiveTab('debug')}
          >
            Debug
          </button>
        </nav>
      </header>

      <main className="app-main">
        {activeTab === 'inference' && <InferencePanel />}
        {activeTab === 'models' && <ModelsPanel />}
        {activeTab === 'debug' && <DebugPanel />}
      </main>
    </div>
  );
}

export default App;
