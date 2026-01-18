import React, { useEffect, useState } from 'react';
import api from '../services/api';
import Table from '../components/Table';

const Merchants = () => {
    const [merchants, setMerchants] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showModal, setShowModal] = useState(false);
    const [formData, setFormData] = useState({
        email: '',
        business_name: '',
        mpesa_till: '',
        country: 'Kenya'
    });
    const [submitting, setSubmitting] = useState(false);
    const [message, setMessage] = useState(null);

    const fetchMerchants = async () => {
        try {
            const res = await api.get('/merchants');
            setMerchants(res.data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchMerchants();
    }, []);

    const handleDelete = async (row) => {
        if (!window.confirm(`Delete merchant ${row.business_name}?`)) return;
        try {
            await api.delete(`/merchants/${row.merchant_id}`);
            fetchMerchants();
        } catch (err) {
            alert('Failed to delete: ' + err.message);
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setSubmitting(true);
        setMessage(null);

        try {
            const res = await api.post('/merchants', formData);
            setMessage({ type: 'success', text: `Merchant "${res.data.business_name}" created (ID: ${res.data.merchant_id})` });
            setFormData({ email: '', business_name: '', mpesa_till: '', country: 'Kenya' });
            fetchMerchants();
            setTimeout(() => setShowModal(false), 1500);
        } catch (err) {
            setMessage({ type: 'error', text: err.response?.data?.error || 'Failed to create merchant' });
        } finally {
            setSubmitting(false);
        }
    };

    const columns = [
        { header: 'ID', key: 'merchant_id' },
        { header: 'Business Name', key: 'business_name', render: r => <span className="font-bold text-white">{r.business_name}</span> },
        { header: 'Owner Email', key: 'users.email', render: r => <span className="text-fintech-primary text-sm">{r['users.email']}</span> },
        { header: 'Till Number', key: 'mpesa_till' },
        { header: 'Country', key: 'country' },
        {
            header: 'Status', key: 'status', render: r => (
                <span className={`px-2 py-0.5 rounded text-xs ${r.status === 'active' ? 'bg-fintech-success/10 text-fintech-success' : 'text-gray-500'}`}>
                    {r.status}
                </span>
            )
        }
    ];

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <h2 className="text-2xl font-bold text-white">Merchants</h2>
                    <p className="text-gray-400">Manage registered businesses</p>
                </div>
                <button
                    onClick={() => { setShowModal(true); setMessage(null); }}
                    className="px-4 py-2 bg-fintech-primary text-white rounded-lg hover:bg-fintech-primary/80 transition font-medium"
                >
                    + Add Merchant
                </button>
            </div>

            <Table
                columns={columns}
                data={merchants}
                loading={loading}
                onDelete={handleDelete}
            />

            {/* Add Merchant Modal */}
            {showModal && (
                <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
                    <div className="bg-fintech-card border border-fintech-border rounded-xl p-6 w-full max-w-md mx-4 shadow-2xl">
                        <h3 className="text-xl font-bold text-white mb-4">Add New Merchant</h3>

                        {message && (
                            <div className={`mb-4 p-3 rounded ${message.type === 'success' ? 'bg-fintech-success/20 text-fintech-success' : 'bg-red-500/20 text-red-400'}`}>
                                {message.text}
                            </div>
                        )}

                        <form onSubmit={handleSubmit} className="space-y-4">
                            <div>
                                <label className="block text-gray-400 text-sm mb-1">Business Name *</label>
                                <input
                                    type="text"
                                    value={formData.business_name}
                                    onChange={(e) => setFormData({ ...formData, business_name: e.target.value })}
                                    className="w-full bg-fintech-dark border border-fintech-border rounded-lg px-4 py-2 text-white focus:outline-none focus:border-fintech-primary"
                                    required
                                />
                            </div>
                            <div>
                                <label className="block text-gray-400 text-sm mb-1">Owner Email *</label>
                                <input
                                    type="email"
                                    value={formData.email}
                                    onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                                    className="w-full bg-fintech-dark border border-fintech-border rounded-lg px-4 py-2 text-white focus:outline-none focus:border-fintech-primary"
                                    required
                                />
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="block text-gray-400 text-sm mb-1">M-Pesa Till</label>
                                    <input
                                        type="text"
                                        value={formData.mpesa_till}
                                        onChange={(e) => setFormData({ ...formData, mpesa_till: e.target.value })}
                                        className="w-full bg-fintech-dark border border-fintech-border rounded-lg px-4 py-2 text-white focus:outline-none focus:border-fintech-primary"
                                    />
                                </div>
                                <div>
                                    <label className="block text-gray-400 text-sm mb-1">Country</label>
                                    <input
                                        type="text"
                                        value={formData.country}
                                        onChange={(e) => setFormData({ ...formData, country: e.target.value })}
                                        className="w-full bg-fintech-dark border border-fintech-border rounded-lg px-4 py-2 text-white focus:outline-none focus:border-fintech-primary"
                                    />
                                </div>
                            </div>

                            <div className="flex gap-3 pt-2">
                                <button
                                    type="button"
                                    onClick={() => setShowModal(false)}
                                    className="flex-1 px-4 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 transition"
                                >
                                    Cancel
                                </button>
                                <button
                                    type="submit"
                                    disabled={submitting}
                                    className="flex-1 px-4 py-2 bg-fintech-primary text-white rounded-lg hover:bg-fintech-primary/80 transition disabled:opacity-50"
                                >
                                    {submitting ? 'Creating...' : 'Create Merchant'}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Merchants;
