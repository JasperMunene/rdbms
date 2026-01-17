import React, { useEffect, useState } from 'react';
import api from '../services/api';
import Table from '../components/Table';
import { motion, AnimatePresence } from 'framer-motion';

const Customers = () => {
    const [customers, setCustomers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [formData, setFormData] = useState({ phone: '', full_name: '', email: '' });

    const fetchCustomers = async () => {
        try {
            const res = await api.get('/customers');
            setCustomers(res.data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchCustomers();
    }, []);

    const handleDelete = async (row) => {
        if (!window.confirm(`Delete customer ${row.full_name}?`)) return;
        try {
            // Need ID for safe delete, row should have customer_id
            // If API used phone, use phone. server/routes uses identifier.
            await api.delete(`/customers/${row.customer_id}`);
            fetchCustomers();
        } catch (err) {
            console.error(err);
            alert('Failed to delete');
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        try {
            await api.post('/customers', formData);
            setShowModal(false);
            setFormData({ phone: '', full_name: '', email: '' });
            fetchCustomers();
        } catch (err) {
            alert('Error creating customer');
        }
    };

    const columns = [
        { header: 'ID', key: 'customer_id' },
        { header: 'Full Name', key: 'full_name', render: r => <span className="font-medium text-white">{r.full_name}</span> },
        { header: 'Phone', key: 'phone', render: r => <span className="font-mono text-fintech-secondary">{r.phone}</span> },
        { header: 'Email', key: 'email' },
        { header: 'Joined', key: 'registration_date', render: r => <span className="text-xs text-gray-500">{r.registration_date}</span> }
    ];

    return (
        <div className="space-y-6 relative">
            <div className="flex justify-between items-center">
                <div>
                    <h2 className="text-2xl font-bold text-white">Customers</h2>
                    <p className="text-gray-400">Verified identity database</p>
                </div>
                <button
                    onClick={() => setShowModal(true)}
                    className="px-4 py-2 bg-fintech-primary text-black font-bold rounded-lg hover:bg-fintech-primary/90 transition-colors"
                >
                    + Add Customer
                </button>
            </div>

            <Table
                columns={columns}
                data={customers}
                loading={loading}
                onDelete={handleDelete}
            />

            {/* Modal */}
            <AnimatePresence>
                {showModal && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
                        <motion.div
                            initial={{ scale: 0.9, opacity: 0 }}
                            animate={{ scale: 1, opacity: 1 }}
                            exit={{ scale: 0.9, opacity: 0 }}
                            className="bg-fintech-card border border-white/10 p-8 rounded-2xl w-full max-w-md shadow-2xl"
                        >
                            <h3 className="text-xl font-bold text-white mb-6">Add New Customer</h3>
                            <form onSubmit={handleSubmit} className="space-y-4">
                                <div>
                                    <label className="text-sm text-gray-400">Full Name</label>
                                    <input
                                        required
                                        className="w-full bg-black/30 border border-white/10 rounded p-2 text-white"
                                        value={formData.full_name}
                                        onChange={e => setFormData({ ...formData, full_name: e.target.value })}
                                    />
                                </div>
                                <div>
                                    <label className="text-sm text-gray-400">Phone</label>
                                    <input
                                        required
                                        className="w-full bg-black/30 border border-white/10 rounded p-2 text-white"
                                        value={formData.phone}
                                        onChange={e => setFormData({ ...formData, phone: e.target.value })}
                                    />
                                </div>
                                <div>
                                    <label className="text-sm text-gray-400">Email (Optional)</label>
                                    <input
                                        type="email"
                                        className="w-full bg-black/30 border border-white/10 rounded p-2 text-white"
                                        value={formData.email}
                                        onChange={e => setFormData({ ...formData, email: e.target.value })}
                                    />
                                </div>
                                <div className="flex justify-end gap-3 mt-6">
                                    <button
                                        type="button"
                                        onClick={() => setShowModal(false)}
                                        className="px-4 py-2 text-gray-400 hover:text-white"
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        type="submit"
                                        className="px-4 py-2 bg-fintech-primary text-black font-bold rounded hover:opacity-90"
                                    >
                                        Save Customer
                                    </button>
                                </div>
                            </form>
                        </motion.div>
                    </div>
                )}
            </AnimatePresence>
        </div>
    );
};

export default Customers;
