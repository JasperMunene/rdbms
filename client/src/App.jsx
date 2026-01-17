import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Layout from './layout/Layout';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import POSTerminal from './pages/POSTerminal';
import Merchants from './pages/Merchants';
import Customers from './pages/Customers';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/login" element={<Login />} />

        {/* Protected Routes */}
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="pos" element={<POSTerminal />} />
          <Route path="transactions" element={<Dashboard />} />
          <Route path="merchants" element={<Merchants />} />
          <Route path="customers" element={<Customers />} />
        </Route>
      </Routes>
    </Router>
  );
}

export default App;
