import React, { useMemo } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

const LiveEventChart = ({ timeSeries }) => {
    // Translate flattened SQL metrics array to chart series
    // The SQL data: { window_start: "...", metric_name: "click", metric_value: 100 }
    const data = useMemo(() => {
        if (!timeSeries || timeSeries.length === 0) return [];

        // Group by window_start
        const grouped = timeSeries.reduce((acc, row) => {
            const timeStr = row.window_start.substring(11, 16); // Extract HH:mm
            if (!acc[timeStr]) {
                acc[timeStr] = { time: timeStr, click_count: 0, purchase_count: 0, revenue: 0 };
            }
            if (row.metric_name === 'click_count' || row.metric_name === 'click') {
                acc[timeStr].click_count = parseFloat(row.metric_value) || 0;
            }
            if (row.metric_name === 'purchase_count' || row.metric_name === 'purchase') {
                acc[timeStr].purchase_count = parseFloat(row.metric_value) || 0;
            }
            if (row.metric_name === 'revenue' || row.metric_name === 'revenue_1m') {
                acc[timeStr].revenue = parseFloat(row.metric_value) || 0;
            }
            return acc;
        }, {});

        return Object.values(grouped).sort((a, b) => a.time.localeCompare(b.time));
    }, [timeSeries]);

    return (
        <div className="glass-card" style={{ height: '400px', display: 'flex', flexDirection: 'column' }}>
            <h3 className="card-title">Live Event Stream (30 mins)</h3>
            <div style={{ flex: 1, marginTop: '20px' }}>
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data}>
                        <defs>
                            <linearGradient id="colorClick" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4} />
                                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                            </linearGradient>
                            <linearGradient id="colorPurchase" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#10b981" stopOpacity={0.4} />
                                <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                        <XAxis dataKey="time" stroke="#64748b" tick={{ fill: '#64748b' }} fontSize={12} stopColor='white' tickMargin={10} axisLine={false} tickLine={false} />
                        <YAxis stroke="#64748b" tick={{ fill: '#64748b' }} fontSize={12} tickMargin={10} axisLine={false} tickLine={false} />
                        <Tooltip
                            contentStyle={{ backgroundColor: 'rgba(15, 17, 26, 0.9)', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '8px' }}
                            itemStyle={{ color: '#e2e8f0' }}
                        />
                        <Area type="monotone" name="Clicks" dataKey="click_count" stroke="#3b82f6" fillOpacity={1} fill="url(#colorClick)" strokeWidth={2} />
                        <Area type="monotone" name="Purchases" dataKey="purchase_count" stroke="#10b981" fillOpacity={1} fill="url(#colorPurchase)" strokeWidth={2} />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};

export default LiveEventChart;
