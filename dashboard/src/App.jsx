import { useWebSocket } from './hooks/useWebSocket';
import { useMetrics } from './hooks/useMetrics';
import KPICard from './components/KPICard';
import LiveEventChart from './components/LiveEventChart';
import TopProductsTable from './components/TopProductsTable';
import DailyReport from './components/DailyReport';
import SystemStatusBar from './components/SystemStatusBar';
import { Activity, Users, ShoppingCart, DollarSign } from 'lucide-react';

function App() {
  const ws = useWebSocket();
  const rest = useMetrics();

  // Prefer WebSocket data for live summary, fallback to REST
  const summary = ws.data?.data || rest.summary;
  const topProducts = ws.data?.top_products || rest.topProducts;
  
  const fmtNumber = (num) => Math.floor(parseFloat(num || 0)).toLocaleString();

  return (
    <div className="dashboard-container">
      {/* Header */}
      <header className="dashboard-header">
        <div className="header-title">
          <Activity size={24} color="#60a5fa" />
          Real-time E-commerce Analytics
        </div>
        <SystemStatusBar wsStatus={ws.status} summary={summary} />
      </header>

      {/* Main Content */}
      <main className="dashboard-content">
        {/* KPI Grid */}
        <div className="grid-kpi">
          <KPICard 
            title="Total Events (1m)" 
            value={fmtNumber(summary?.events_1m || summary?.click_1m)} 
            icon={<Activity size={18} />} 
          />
          <KPICard 
            title="Active Users (1m)" 
            value={fmtNumber(summary?.active_users_1m)} 
            icon={<Users size={18} />} 
          />
          <KPICard 
            title="Revenue (1m)" 
            value={fmtNumber(summary?.revenue_1m)} 
            suffix=" đ"
            icon={<DollarSign size={18} />} 
          />
          <KPICard 
            title="Purchase/Cart" 
            value={fmtNumber(summary?.purchase_1m)} 
            icon={<ShoppingCart size={18} />} 
          />
        </div>

        {/* Charts Grid */}
        <div className="grid-charts">
          <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
            <LiveEventChart timeSeries={rest.timeSeries} />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
            <TopProductsTable topProducts={topProducts} />
            <DailyReport dailyData={rest.dailyReport} />
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
