import React, { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import api from '../services/api';

const Dashboard = () => {
    const [stats, setStats] = useState({ total_volume: 0, transaction_count: 0, active_merchants: 0 });
    const [transactions, setTransactions] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const [statsRes, txRes] = await Promise.all([
                    api.get('/stats'),
                    api.get('/transactions')
                ]);
                setStats(statsRes.data);
                setTransactions(txRes.data);
            } catch (error) {
                console.error("Failed to fetch dashboard data", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, []);

    const statCards = [
        { label: 'Total Volume', value: `KES ${stats.total_volume?.toLocaleString()}`, color: 'from-fintech-primary to-blue-500' },
        { label: 'Transactions', value: stats.transaction_count, color: 'from-purple-500 to-fintech-secondary' },
        { label: 'Active Merchants', value: stats.active_merchants, color: 'from-fintech-success to-emerald-600' }
    ];

    return (
        <div className="space-y-8">
            <header>
                <h2 className="text-3xl font-bold text-white">Merchant Command Center</h2>
                <p className="text-gray-400">Real-time database analytics</p>
            </header>

            {/* Stats Grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {statCards.map((card, idx) => (
                    <motion.div
                        key={idx}
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: idx * 0.1 }}
                        className={`glass-card p-6 rounded-xl bg-gradient-to-br ${card.color} bg-opacity-10 border border-white/5 relative overflow-hidden`}
                    >
                        <div className="absolute top-0 right-0 p-4 opacity-10">
                            <div className="w-16 h-16 rounded-full bg-white blur-xl"></div>
                        </div>
                        <h3 className="text-sm font-medium text-white/70 uppercase tracking-wider">{card.label}</h3>
                        <p className="text-3xl font-bold text-white mt-2">{card.value}</p>
                    </motion.div>
                ))}
            </div>

            {/* Transactions Table - JOIN Showcase */}
            <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.4 }}
                className="glass-panel rounded-xl overflow-hidden"
            >
                <div className="p-6 border-b border-white/10 flex justify-between items-center">
                    <div>
                        <h3 className="text-xl font-bold text-white">Recent Transactions</h3>
                        <p className="text-xs text-fintech-primary mt-1">
                            ⚠️ Demonstrating <span className="font-mono bg-white/10 px-1 rounded">INNER JOIN merchants</span> & <span className="font-mono bg-white/10 px-1 rounded">LEFT JOIN customers</span>
                        </p>
                    </div>
                </div>

                <div className="overflow-x-auto">
                    <table className="w-full text-left">
                        <thead className="bg-white/5 text-xs text-gray-400 uppercase tracking-wider">
                            <tr>
                                <th className="px-6 py-4">ID</th>
                                <th className="px-6 py-4 text-fintech-primary">Business (Joined)</th>
                                <th className="px-6 py-4 text-fintech-secondary">Customer (Joined)</th>
                                <th className="px-6 py-4 text-right">Amount</th>
                                <th className="px-6 py-4 text-center">Status</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                            {loading ? (
                                <tr><td colSpan="5" className="p-8 text-center text-gray-500">Loading live data...</td></tr>
                            ) : transactions.length === 0 ? (
                                <tr><td colSpan="5" className="p-8 text-center text-gray-500">No transactions found</td></tr>
                            ) : (
                                transactions.map((tx) => (
                                    <tr key={tx.transaction_id} className="hover:bg-white/5 transition-colors">
                                        <td className="px-6 py-4 text-sm font-mono text-gray-400">#{tx.reference || tx.transaction_id}</td>

                                        {/* Joined Merchant Data */}
                                        <td className="px-6 py-4">
                                            <span className="text-white font-medium">{tx['merchants.business_name']}</span>
                                            <div className="text-xs text-fintech-primary/70">ID: {tx.merchant_id}</div>
                                        </td>

                                        {/* Joined Customer Data */}
                                        <td className="px-6 py-4">
                                            {tx['customers.full_name'] ? (
                                                <>
                                                    <div className="text-white">{tx['customers.full_name']}</div>
                                                    <div className="text-xs text-fintech-secondary/70">{tx['customers.phone']}</div>
                                                </>
                                            ) : (
                                                <span className="text-gray-500 italic">Guest / Unlinked</span>
                                            )}
                                        </td>

                                        <td className="px-6 py-4 text-right font-mono text-fintech-success">
                                            KES {parseFloat(tx.amount).toLocaleString()}
                                        </td>
                                        <td className="px-6 py-4 text-center">
                                            <span className={`px-2 py-1 rounded text-xs border ${tx.status === 'completed'
                                                ? 'bg-fintech-success/10 text-fintech-success border-fintech-success/20'
                                                : 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'
                                                }`}>
                                                {tx.status}
                                            </span>
                                        </td>
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
            </motion.div>
        </div>
    );
};

export default Dashboard;
