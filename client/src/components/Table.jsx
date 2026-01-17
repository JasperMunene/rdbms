import React from 'react';
import { motion } from 'framer-motion';

const Table = ({ columns, data, loading, onRowClick, onDelete }) => {
    return (
        <div className="glass-panel rounded-xl overflow-hidden">

            <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                    <thead className="bg-white/5 text-xs text-gray-400 uppercase tracking-wider">
                        <tr>
                            {columns.map((col, i) => (
                                <th key={i} className="px-6 py-4">{col.header}</th>
                            ))}
                            <th className="px-6 py-4 text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-white/5">
                        {loading ? (
                            <tr><td colSpan={columns.length + 1} className="p-8 text-center text-gray-500">Loading...</td></tr>
                        ) : data.length === 0 ? (
                            <tr><td colSpan={columns.length + 1} className="p-8 text-center text-gray-500">No records found</td></tr>
                        ) : (
                            data.map((row) => (
                                <motion.tr
                                    key={row.id || Math.random()}
                                    initial={{ opacity: 0 }}
                                    animate={{ opacity: 1 }}
                                    className="hover:bg-white/5 transition-colors group"
                                >
                                    {columns.map((col, i) => (
                                        <td key={i} className="px-6 py-4 whitespace-nowrap">
                                            {col.render ? col.render(row) : row[col.key]}
                                        </td>
                                    ))}
                                    <td className="px-6 py-4 text-right">
                                        <button
                                            onClick={(e) => { e.stopPropagation(); onDelete(row); }}
                                            className="text-red-500/50 hover:text-red-500 hover:bg-red-500/10 px-3 py-1 rounded transition-all text-sm opacity-0 group-hover:opacity-100"
                                        >
                                            Delete
                                        </button>
                                    </td>
                                </motion.tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default Table;
