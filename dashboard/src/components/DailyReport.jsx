import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

const DailyReport = ({ dailyData }) => {
    const data = dailyData.map(d => ({
        date: d.date.substring(5, 10), // MM-DD
        revenue: parseFloat(d.total_revenue) / 1000 // Convert to K
    })).reverse();

    return (
        <div className="glass-card glass-card-secondary" style={{ height: '300px' }}>
            <h3 className="card-title">Weekly Revenue (Batch)</h3>
            <ResponsiveContainer width="100%" height="80%">
                <BarChart data={data}>
                    <XAxis dataKey="date" stroke="#64748b" tick={{ fill: '#64748b' }} axisLine={false} tickLine={false} fontSize={12} />
                    <YAxis stroke="#64748b" tick={{ fill: '#64748b' }} axisLine={false} tickLine={false} fontSize={12} />
                    <Tooltip
                        cursor={{ fill: 'rgba(255,255,255,0.05)' }}
                        contentStyle={{ backgroundColor: 'rgba(15, 17, 26, 0.9)', border: '1px solid rgba(255,255,255,0.1)' }}
                    />
                    <Bar name="Rev (k)" dataKey="revenue" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default DailyReport;
